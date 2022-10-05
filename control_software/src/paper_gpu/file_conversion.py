# -*- coding: utf-8 -*-
# Copyright (c) 2022 The HERA Collaboration
# Licensed under the 2-clause BSD License

import time
import h5py
import redis
import warnings
import numpy as np
import cartopy.crs as ccrs
import pyuvdata.utils as uvutils
from hera_mc import geo_sysdef
from hera_corr_cm import redis_cm


no_bitshuffle_message = (
    "bitshuffle is not installed. Install with `pip install .[bitshuffle]` "
    "from the top level. Writing without bitshuffle."
)
have_bitshuffle = True
try:
    import hdf5plugin
except ImportError:
    have_bitshuffle = False

# define correlator type
_hera_corr_dtype = np.dtype([("r", "<i4"), ("i", "<i4")])

# define Easting/Northing magic numbers
# HERA is in Zone 34J; corresponds to latitude 10000000 in northings
UTM_TILE = 34
LAT_CORR = 10000000


def read_header_data(filename):
    """
    Read metadata from hdf5 file written by correlator.

    Parameters
    ----------
    filename : str
        The filename of the metadata file.

    Returns
    -------
    dict
        A dict containing: t0 (starting time; unsigned integer), mcnt (mcnt
        corresponding to last count of data; unsigned integer), nfreq (number of
        frequency channels in data; unsigned integer), nstokes (number of
        stokes/polarizations; unsigned integer), corr_ver (correlator git
        version; string), tag (observation tag; string), ant_0_array (list of
        first antennas in data; 1-d array of ints of length nblts), ant_1_array
        (list of second antennas in data; 1-d array of ints of length nblts),
        time_array (JD of observation; 1-d array of floats of length nblts),
        integration_time (seconds of observation; 1-d array of floats of length
        nblts). Dict keys are the names of these quantities.
    """
    # pull data from HDF5 metadata file
    meta_dict = {}
    with h5py.File(filename, "r") as h5f:
        meta_dict["t0"] = h5f["t0"][()]
        meta_dict["mcnt"] = h5f["mcnt"][()]
        meta_dict["nfreq"] = h5f["nfreq"][()]
        meta_dict["nstokes"] = h5f["nstokes"][()]
        meta_dict["corr_ver"] = h5f["corr_ver"][()].decode("utf-8")
        meta_dict["tag"] = h5f["tag"][()].decode("utf-8")
        meta_dict["ant_0_array"] = h5f["ant_0_array"][()]
        meta_dict["ant_1_array"] = h5f["ant_1_array"][()]
        meta_dict["time_array"] = h5f["time_array"][()]
        meta_dict["integration_time"] = h5f["integration_time"][()]

    return meta_dict


def read_data_file(filename, data_shape):
    """
    Read a block of binary data from file.

    Parameters
    ----------
    filename : str
        The name of the file to read.
    data_shape : tuple of int
        The expected size of the data. Data read will be reshaped to this.

    Returns
    -------
    data : nd-array
        The data that was read. Compound numpy datatype with a "r" field and "i"
        field, both 32-bit integers.

    Raises
    ------
    ValueError
        Raised if the data read in cannot be reshaped into the specified shape.
    """
    # read raw binary data
    data = np.fromfile(filename, dtype=_hera_corr_dtype)
    try:
        data = data.reshape(data_shape)
    except ValueError:
        raise ValueError(
            f"data cannot be reshaped; data read is {data.size} elements, "
            "target size is {data_shape}"
        )

    # return to user
    return data


def get_antpos_info():
    """
    Fetch HERA antenna positions from hera_mc.

    Parameters
    ----------
    None

    Returns
    -------
    antpos_xyz : ndarray
        An array of floats of size (350, 3) which contains the antenna
        positions in XYZ (i.e., ECEF) coordinates.
    ant_names : ndarray
        An array of strings of size (350,) which contains the antenna names.
    """
    # read antenna positions from M&C
    ants = geo_sysdef.read_antennas()
    # convert from eastings/northings to ENU
    latlon_p = ccrs.Geodetic()
    utm_p = ccrs.UTM(UTM_TILE)
    antpos_xyz = np.empty((350, 3), dtype=np.float64)
    ant_names = np.empty((350,), dtype="S5")
    for ant, pos in ants.items():
        # unpack antenna information
        antnum = int(ant[2:])
        if antnum > 350:
            # skip bizarre HT701 entry
            continue
        easting = pos["E"]
        northing = pos["N"]
        elevation = pos["elevation"]
        # transform coordinates to ECEF
        lon, lat = latlon_p.transform_point(easting, northing - LAT_CORR, utm_p)
        xyz = uvutils.XYZ_from_LatLonAlt(np.radians(lat), np.radians(lon), elevation)
        antpos_xyz[antnum, :] = xyz
        # also save antenna names
        ant_names[antnum] = ant

    return antpos_xyz, ant_names


def make_uvh5_file(filename, metadata_file, data_file):
    """
    Make a UVH5 file from a metdata + raw binary data file.

    This function creates a valid UVH5 file from the specified metadata and
    binary data files. It adds the flags and nsample datasets, and applies
    bitshuffle compression to the data.

    Parameters
    ----------
    filename : str
        The name of the output file to write.
    metadata_file : str
        The name of the metadata file written by the correlator.
    data_file : str
        The name of the data file written by the correlator.

    Returns
    -------
    metadata : dict
        Metadata read from the meta hdf5 file
    """
    # get cminfo from redis
    cminfo = redis_cm.read_cminfo_from_redis(return_as="dict")

    # get antennas positions and names from mc
    antpos_xyz, ant_names = get_antpos_info()
    nants_telescope = len(ant_names)

    # read in metadata
    metadata = read_header_data(metadata_file)
    t0 = metadata["t0"]
    mcnt = metadata["mcnt"]
    nfreq = metadata["nfreq"]
    nstokes = metadata["nstokes"]
    corr_ver = metadata["corr_ver"]
    tag = metadata["tag"]
    ant_0_array = metadata["ant_0_array"]
    ant_1_array = metadata["ant_1_array"]
    time_array = metadata["time_array"]
    integration_time = metadata["integration_time"]

    # make sure metadata are the right size
    nblts = ant_0_array.shape[0]
    actual_shapes = np.array(
        [ant_1_array.shape[0], time_array.shape[0], integration_time.shape[0]]
    )
    if np.any(actual_shapes != nblts):
        raise ValueError(
            f"one or more bad data shapes; expected {nblts}, got {actual_shapes}"
        )

    # compute other necessary metadata
    baseline_array = uvutils.antnums_to_baseline(
        ant_0_array, ant_1_array, nants_telescope
    )
    nbls = len(baseline_array)
    ant_nums = np.asarray([int(name[2:]) for name in ant_names])
    antenna_diameters = 14.0 * np.ones((len(ant_names),), dtype=np.float64)
    cofa_lat_rad = cminfo["cofa_lat"] * np.pi / 180.0
    cofa_lon_rad = cminfo["cofa_lon"] * np.pi / 180.0
    altitude = cminfo["cofa_alt"]
    cofa_xyz = uvutils.XYZ_from_LatLonAlt(cofa_lat_rad, cofa_lon_rad, altitude)
    antpos_xyz -= cofa_xyz
    # the uvw calculation will have to change when we turn fringe stopping on
    uvw_array = uvutils.calc_uvw(
        use_ant_pos=True,
        antenna_positions=antpos_xyz,
        antenna_numbers=ant_nums,
        ant_1_array=ant_0_array,
        ant_2_array=ant_1_array,
        telescope_lat=cofa_lat_rad,
        telescope_lon=cofa_lon_rad,
        to_enu=True,
    )

    # build frequency information from redis
    rd = redis.Redis("redishost", decode_responses=True)
    sample_freq = float(rd["feng:sample_freq"])
    nchans_f = int(rd["feng:samples_per_mcnt"]) // 2
    bandwidth = sample_freq / 2.0
    nchan_sum = 4  # averaging 4 channels together; may change in future
    nchans = int(nchans_f // nchan_sum * 3 // 4)  # number of channels we're writing
    start_chan = nchans_f // 16 * 3
    channel_width = bandwidth / nchans_f * nchan_sum
    freqs = np.linspace(0, bandwidth, nchans_f + 1)
    freqs = freqs[start_chan : start_chan + (nchans_f // 4 * 3)]  # downselect
    freqs = freqs.reshape(nchans, nchan_sum).sum(axis=1) / nchan_sum
    channel_width = channel_width * np.ones_like(freqs)  # need an array

    # define the size of the data array
    data_shape = (nblts, nfreq, nstokes)

    # read in raw data
    data = read_data_file(data_file, data_shape)

    # save in UVH5 file
    with h5py.File(filename, "w") as h5f:
        # make datagroups
        header_dgrp = h5f.create_group("Header")
        eq_dgrp = header_dgrp.create_group("extra_keywords")
        data_dgrp = h5f.create_group("Data")

        # write header info
        # telescope + phasing info
        header_dgrp["latitude"] = cminfo["cofa_lat"]
        header_dgrp["longitude"] = cminfo["cofa_lon"]
        header_dgrp["altitude"] = cminfo["cofa_alt"]
        header_dgrp["telescope_name"] = np.bytes_("HERA")
        header_dgrp["instrument"] = np.bytes_("HERA")
        header_dgrp["object_name"] = np.bytes_("zenith")
        header_dgrp["phase_type"] = np.bytes_("drift")

        # required UVParameters
        header_dgrp["Nants_data"] = len(np.unique(ant_0_array))
        header_dgrp["Nants_telescope"] = len(ant_names)
        header_dgrp["Nbls"] = nbls
        header_dgrp["Nblts"] = nblts
        header_dgrp["Nfreqs"] = nfreq
        header_dgrp["Npols"] = nstokes
        header_dgrp["Nspws"] = 1  # might change when doing polarization transpose
        header_dgrp["Ntimes"] = len(np.unique(time_array))
        header_dgrp["antenna_numbers"] = ant_nums
        header_dgrp["uvw_array"] = uvw_array
        header_dgrp["vis_units"] = np.bytes_("uncalib")
        header_dgrp["channel_width"] = channel_width
        header_dgrp["time_array"] = time_array
        header_dgrp["freq_array"] = freqs
        header_dgrp["integration_time"] = integration_time
        header_dgrp["polarization_array"] = np.asarray([-5, -6, -7, -8])
        header_dgrp["spw_array"] = np.asarray([0])
        header_dgrp["ant_1_array"] = ant_0_array
        header_dgrp["ant_2_array"] = ant_1_array
        header_dgrp["antenna_positions"] = antpos_xyz
        header_dgrp["flex_spw"] = False  # might change with polarization transpose
        header_dgrp["multi_phase_center"] = False  # will change with fringe stopping
        header_dgrp["antenna_names"] = np.asarray(ant_names, dtype="bytes")
        header_dgrp["history"] = np.bytes_(
            "Written by the HERA Correlator on " + time.ctime() + "."
        )

        # optional parameters
        header_dgrp["x_orientation"] = np.bytes_("north")
        header_dgrp["antenna_diameters"] = antenna_diameters

        # extra keywords
        eq_dgrp["t0"] = t0
        eq_dgrp["mcnt"] = mcnt
        eq_dgrp["corr_ver"] = np.bytes_(corr_ver)
        eq_dgrp["tag"] = np.bytes_(tag)

        # write data
        data_chunks = (128, nfreq, 1)  # assuming Nfreq = 1536, chunks are ~1 MB in size
        compression_filter = 32008  # bitshuffle filter number
        block_size = 0  # let bitshuffle decide
        compression_opts = (block_size, 2)  # use LZ4 compression after bitshuffle

        if have_bitshuffle:
            visdata_dset = data_dgrp.create_dataset(
                "visdata",
                chunks=data_chunks,
                data=data,
                compression=compression_filter,
                compression_opts=compression_opts,
                dtype=_hera_corr_dtype,
            )
        else:
            warnings.warn(no_bitshuffle_message)
            visdata_dset = data_dgrp.create_dataset(
                "visdata",
                chunks=data_chunks,
                data=data,
                dtype=_hera_corr_dtype,
            )

        # also write flags and nsamples
        flags = np.zeros_like(data, dtype=np.bool_)
        flags_dset = data_dgrp.create_dataset(
            "flags",
            chunks=data_chunks,
            data=flags,
            dtype="b1",
            compression="lzf",
        )
        nsamples = np.ones_like(data, dtype=np.float32)
        nsamples_dset = data_dgrp.create_dataset(
            "nsamples",
            chunks=data_chunks,
            data=nsamples,
            dtype=np.float32,
            compression="lzf",
        )

    # we're done!
    return metadata

def check_file(filename):
    '''Makes sure a converted file and has expected data/flag/nsample arrays 
    with the right shapes and types.

    Arguments:
        filename: full path to .uvh5 file to check.
    Returns:
        None (but will error if something is found to be wrong with the file).
    '''
    # check that f['/Data'] has three keys
    f = h5py.File(filename, 'r')
    assert len(f['/Data']) == 3

    # check that arrays are the right shape
    expected_shape = (f['/Header']['Nblts'][()], 
                      f['/Header']['Nfreqs'][()], 
                      f['/Header']['Npols'][()])
    assert f['/Data']['visdata'].shape == expected_shape
    assert f['/Data']['flags'].shape == expected_shape
    assert f['/Data']['nsamples'].shape == expected_shape

    # check that the arrays are the right type
    assert f['/Data']['visdata'].dtype == [('r', '<i4'), ('i', '<i4')]
    assert f['/Data']['flags'].dtype == 'bool'
    assert f['/Data']['nsamples'].dtype == '<f4'
