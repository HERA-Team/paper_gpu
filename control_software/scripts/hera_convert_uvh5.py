#!/usr/bin/env python
# -*- coding: utf-8 -*-

import h5py
import numpy as np
import cartopy.crs as ccrs
import pyuvdata.utils as uvutils
from hera_mc import geo_sysdef
from hera_corr_cm import redis_cm
import bitshuffle.h5

# define correlator dtype
hera_corr_dtype = np.dtype([("r", "<i4"), ("i", "<i4")])


def read_header_data(filename):
    """
    Read metadata from hdf5 file written by correlator.

    Parameters
    ----------
    filename : str
        The filename of the metadata file.

    Returns
    -------
    tuple
        A tuple containing: t0 (starting time; unsigned integer), mcnt (mcnt
        corresponding to last count of data; unsigned integer), nfreq (number of
        frequency channels in data; unsigned integer), nstokes (number of
        stokes/polarizations; unsigned integer), corr_ver (correlator git
        version; string), ant_0_array (list of first antennas in data; 1-d array
        of ints of length nblts), ant_1_array (list of second antennas in data;
        1-d array of ints of length nblts), time_array (JD of observation; 1-d
        array of floats of length nblts), integration_time (seconds of
        observation; 1-d array of floats of length nblts).
    """
    # pull data from HDF5 metadata file
    with h5py.File(filename, "r") as h5f:
        t0 = h5f["t0"][()]
        mcnt = h5f["mcnt"][()]
        nfreq = h5f["nfreq"][()]
        nstokes = h5f["nstokes"][()]
        corr_ver = h5f["corr_ver"][()].decode("utf-8")
        ant_0_array = h5f["ant_0_array"][()]
        ant_1_array = h5f["ant_1_array"][()]
        time_array = h5f["time_array"][()]
        integration_time = h5f["integration_time"][()]

    return (
        t0,
        mcnt,
        nfreq,
        nstokes,
        corr_ver,
        ant_0_array,
        ant_1_array,
        time_array,
        integration_time,
    )

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
    data = np.fromfile(filename, dtype=hera_corr_dtype)
    try:
        data.reshape(data_shape)
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
    # HERA is in Zone 34J; corresponds to latitude 10000000 in northings
    latlon_p = ccrs.Geodetic()
    utm_p = ccrs.UTM(34)
    lat_corr = 10000000
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
        lon, lat = latlon_p.transform_point(easting, northing - lat_corr, utm_p)
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
    """
    # get cminfo from redis
    cminfo = redis_cm.read_cminfo_from_redis(return_as="dict")

    # get antennas positions and names from mc
    antpos_xyz, ant_names = get_antpos_info()
    nants_telescope = len(ant_names)

    # read in metadata
    metadata = read_header_data(metadata_file)
    t0 = metadata[0]
    mcnt = metadata[1]
    nfreq = metadata[2]
    nstokes = metadata[3]
    corr_ver = metadata[4]
    ant_0_array = metadata[5]
    ant_1_array = metadata[6]
    time_array = metadata[7]
    integration_time = metadata[8]

    # make sure metadata are the right size
    nblts = ant_0_array.shape[0]
    actual_shapes = np.array([ant_1_array.shape[0], time_array.shape[0], integration_time.shape[0]])
    if np.any(actual_shapes != nblts):
        raise ValueError(
            f"one or more bad data shapes; expected {nblts}, got {actual_shapes}"
        )

    # compute other necessary metadata
    baseline_array = uvutils.antnums_to_baseline(ant_0_array, ant_1_array, nants_telescope)
    nbls = len(baseline_array)
    ant_nums = np.asarray([int(name[2:]) for name in ant_names])

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
        header["latitude"] = cminfo["cofa_lat"]
        header["longitude"] = cminfo["cofa_lon"]
        header["altitude"] = cminfo["cofa_alt"]
        header["telescope_name"] = np.bytes_("HERA")
        header["instrument"] = np.bytes_("HERA")
        header["object_name"] = np.bytes_("zenith")
        header["phase_type"] = np.bytes_("unphased")

        # required UVParameters
        header["Nants_data"] = len(np.unique(ant_0_array))
        header["Nants_telescope"] = len(ant_names)
        header["Nbls"] = nbls
        header["Nblts"] = nblts
        header["Nfreqs"] = nfreq
        header["Npols"] = nstokes
        header["Nspws"] = 1  # might change when doing polarization transpose
        header["Ntimes"] = len(np.unique(time_array))
        header["antenna_numbers"] = ant_nums
        header["uvw_array"] = # TODO
        header["vis_units"] = np.bytes_("uncalib")
        header["channel_width"] = # TODO
        header["time_array"] = time_array
        header["freq_array"] = # TODO
        header["integration_time"] = # TODO
        header["polarization_array"] = np.asarray([-5, -6, -7, -8])
        header["spw_array"] = np.asarray([0])
        header["ant_1_array"] = ant_0_array
        header["ant_2_array"] = ant_1_array
        header["antenna_positions"] = antpos_xyz
        header["flex_spw"] = False  # might change with polarization transpose
        header["multi_phase_center"] = False  # will change with fringe stopping
        header["antenna_names"] = np.asarray(ant_names, dtype="bytes")
        header["x_orientation"] = np.bytes_("north")

        # extra keywords
        eq_dgrp["t0"] = t0
        eq_dgrp["mcnt"] = mcnt
        eq_dgrp["corr_ver"] = np.bytes_(corr_ver)

        # write data
        data_chunks = (128, Nfreq, 1)  # assuming Nfreq = 1536, chunks are ~1 MB in size
        visdata_dset = data_dgrp.create_dataset(
            "visdata",
            chunks=data_chunks,
            data=data,
            compression=bitshuffle.h5.H5FILTER,
            compression_opts=(0, bitshuffle.h5.H5_COMPRESS_LZ4),
            dtype=hera_corr_dtype,
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
    return
