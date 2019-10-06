#!/usr/bin/env python

from __future__ import print_function, division, absolute_import
import h5py
import sys
import json
import logging
import numpy as np
import time
import copy
import redis
from hera_corr_f import helpers

logger = helpers.add_default_log_handlers(logging.getLogger(__file__))

def get_corr_to_hera_map(r, nants_data=192, nants=352):
    """
    Given a redis.Redis instance, r, containing
    appropriate metadata - figure out the mapping
    of correlator index (0 - Nants_data -1) to
    hera antenna number (0 - Nants).
    """
    out_map = {k:v for k,v in enumerate(range(nants, nants+nants_data))} # use default values outside the range of real antennas

    # A dictionary with keys which are antenna numbers
    # of the for {<ant> :{<pol>: {'host':SNAPHOSTNAME, 'channel':INTEGER}}}
    ant_to_snap = json.loads(r.hgetall("corr:map")['ant_to_snap'])
    #host_to_index = r.hgetall("corr:snap_ants")
    for ant, pol in ant_to_snap.iteritems():
        hera_ant_number = int(ant)
        host = pol["n"]["host"]
        chan = pol["n"]["channel"] # runs 0-5
        snap_ant_chans = r.hget("corr:snap_ants", host)
        if snap_ant_chans is None:
            logger.warning("Couldn't find antenna indices for %s" % host)
            continue
        corr_ant_number = json.loads(snap_ant_chans)[chan//2] #Indexes from 0-3 (ignores pol)
        print(corr_ant_number)
        out_map[corr_ant_number] = hera_ant_number
        logger.info("HERA antenna %d maps to correlator input %d" % (hera_ant_number, corr_ant_number))

    return out_map

def get_bl_order(n_ants):
    """
    Return the order of baseline data output by a CASPER correlator
    X engine.
    
    Extracted from the corr package -- https://github.com/ska-sa/corr
    """
    order1, order2 = [], []
    for i in range(n_ants):
        for j in range(int(n_ants//2),-1,-1):
            k = (i-j) % n_ants
            if i >= k: order1.append((k, i))
            else: order2.append((i, k))
    order2 = [o for o in order2 if o not in order1]
    return tuple([o for o in order1 + order2])

def get_ant_names():
    """
    Generate a list of antenna names, where position
    in the list indicates numbering in the data files.
    """
    return ["foo"]*352

def get_cm_info():
    from hera_mc import cm_sysutils
    h = cm_sysutils.Handling()
    return h.get_cminfo_correlator()

def get_antpos_enu(antpos, lat, lon, alt):
    """
    Compute the antenna positions in ENU coordinates from rotECEF.

    Args:
      antpos -- array of antenna positions. Should have shape (Nants, 3).
      lat (float) -- telescope latitude, in radians
      lon (float) -- telescope longitude, in radians
      alt (float) -- telescope altitude, in meters

    Returns:
      enu -- array of antenna positions in ENU frame. Has shape (Nants, 3).
    """
    import pyuvdata.utils as uvutils
    ecef = uvutils.ECEF_from_rotECEF(antpos, lon)
    enu  = uvutils.ENU_from_ECEF(ecef, lat, lon, alt)
    return enu

def get_antpos_ecef(antpos, lon):
    """
    Compute the antenna positions in ECEF coordinates from rotECEF

    Args:
      antpos -- array of antenna positions. Should have shape (Nants, 3).
      lon (float) -- telescope longitude, in radians

    Returns:
      ecef -- array of antenna positions in ECEF frame. Has shape (Nants, 3)
    """
    import pyuvdata.utils as uvutils
    ecef = uvutils.ECEF_from_rotECEF(antpos, lon)
    return ecef

def get_telescope_location_ecef(lat, lon, alt):
    """
    Compute the telescope location in ECEF coordinates from lat/lon/alt.

    Args:
      lat (float) -- telescope latitude, in radians
      lon (float) -- telescope longitude, in radians
      alt (float) -- telescope altitude, in meters

    Returns:
       ecef -- len(3) array of x,y,z values of telescope location in ECEF
           coordinates, in meters.
    """
    import pyuvdata.utils as uvutils
    return uvutils.XYZ_from_LatLonAlt(lat, lon, alt)


def create_header(h5, config, use_cm=False, use_redis=False):
    """
    Create an HDF5 file with appropriate datasets in a "Header"
    data group.

    inputs: h5 -- an h5py File object
            use_cm -- boolean. If True, get valid data from the hera_cm
                      system. If False, just stuff the header with fake
                      data.
    """

    INSTRUMENT = "HERA"

    #Load config file
    N_MAX_INTTIME = 8
    config = np.loadtxt(config, dtype=np.int)
    baselines = []
    integration_time = []

    #for time_ctr in range(1,9,1):
    #    for i,t in enumerate(config[:,2]):
    #        if (t==0): continue
    #        elif not (time_ctr % t):
    #            baselines.append((config[i,0], config[i,1]))
    #            integration_time.append(t * 2)

    for i,t in enumerate(config[:,2]):
        if (t!=0):
           baselines.append([(config[i,0], config[i,1])]*(8//t))
           integration_time.append(np.repeat(t*2, int(8//t)))
    baselines = np.concatenate(baselines)
    integration_time = np.asarray(np.concatenate(integration_time), dtype=np.float64)

    ant_1_array = np.array([x for (x,y) in baselines])
    ant_2_array = np.array([y for (x,y) in baselines])

    NANTS_DATA = len(set(ant_1_array))
    NANTS = 352
    NCHANS = int(2048 // 4 * 3)
    NCHANS_F = 8192
    NCHAN_SUM = 4
    ANT_DIAMETER = 14.0
    INT_TIME = 10.0
    n_bls = len(baselines) #bls = np.array(get_bl_order(NANTS_DATA))
    channel_width = 250e6 / NCHANS_F * NCHAN_SUM
    freqs = np.linspace(0, 250e6, NCHANS_F + 1)[1536 : 1536 + (8192 // 4 * 3)]
    # average over channels
    freqs = freqs.reshape(NCHANS, NCHAN_SUM).sum(axis=1) / NCHAN_SUM
    uvw = np.zeros([n_bls, 3])

    if use_cm:
        cminfo = get_cm_info()
        # add the enu co-ords
        lat = cminfo["cofa_lat"] * np.pi / 180.
        lon = cminfo["cofa_lat"] * np.pi / 180.
        cminfo["antenna_positions_enu"] = get_antpos_enu(cminfo["antenna_positions"], lat, lon,
                                                         cminfo["cofa_alt"])
    else:
        cminfo = None

    if use_redis:
        r = redis.Redis("redishost")
        fenginfo = r.hgetall("init_configuration")
        corr_to_hera_map = get_corr_to_hera_map(r, nants_data=NANTS_DATA, nants=NANTS)
        for n in range(baselines.shape[0]):
            baselines[n] = [corr_to_hera_map[baselines[n,0]], corr_to_hera_map[baselines[n,1]]]
    else:
        fenginfo = None
        # Use impossible antenna numbers to indicate they're not really valid
        corr_to_hera_map = np.arange(NANTS, NANTS+NANTS_DATA)

    header = h5.create_group("Header")
    header.create_dataset("Nants_data", dtype="<i8", data=NANTS_DATA)
    header.create_dataset("Nants_telescope", dtype="<i8", data=NANTS)
    header.create_dataset("Nbls",   dtype="<i8", data=n_bls)
    header.create_dataset("Nblts",  dtype="<i8", data=n_bls) 
    header.create_dataset("Nfreqs", dtype="<i8", data=NCHANS)
    header.create_dataset("Npols",  dtype="<i8", data=4)
    header.create_dataset("Nspws",  dtype="<i8", data=1)
    header.create_dataset("Ntimes", dtype="<i8", data=n_bls) 
    header.create_dataset("corr_bl_order", dtype="<i8", data=np.array(baselines))
    header.create_dataset("ant_1_array_conf", dtype="<i8", data=ant_1_array)
    header.create_dataset("ant_2_array_conf", dtype="<i8", data=ant_2_array)
    header.create_dataset("antenna_diameters", dtype="<f8", data=[ANT_DIAMETER] * NANTS)
    header.create_dataset("channel_width",     dtype="<f8", data=channel_width)
    header.create_dataset("freq_array",        dtype="<f8", shape=(1, NCHANS), data=freqs) #TODO Get from config
    header.create_dataset("history",   data=np.string_("%s: Template file created\n" % time.ctime()))
    header.create_dataset("instrument", data=np.string_(INSTRUMENT))
    header.create_dataset("integration_time", dtype="<f8", data=integration_time)
    header.create_dataset("object_name", data=np.string_("zenith"))
    header.create_dataset("phase_type",  data=np.string_("drift"))
    header.create_dataset("polarization_array", dtype="<i8", data=[-5, -6, -7, -8])
    header.create_dataset("spw_array",      dtype="<i8", data=[0])
    header.create_dataset("telescope_name", data=np.string_("HERA"))
    header.create_dataset("vis_units",  data=np.string_("UNCALIB"))
    if use_cm:
        # convert lat and lon from degrees -> radians
        lat = cminfo['cofa_lat'] * np.pi / 180.
        lon = cminfo['cofa_lon'] * np.pi / 180.
        alt = cminfo['cofa_alt']
        telescope_location_ecef = get_telescope_location_ecef(lat, lon, alt)
        antpos_ecef = get_antpos_ecef(cminfo["antenna_positions"], lon)
        header.create_dataset("altitude",    dtype="<f8", data=cminfo['cofa_alt'])
        ant_pos = -1 * np.ones([NANTS,3], dtype=np.int64) * telescope_location_ecef
        ant_pos_enu = -1 * np.ones([NANTS,3], dtype=np.int64) * telescope_location_ecef
        ant_names = ["NONE"]*NANTS
        ant_nums = [-1]*NANTS
        for n, i in enumerate(cminfo["antenna_numbers"]):
            ant_pos[i]     = antpos_ecef[n] - telescope_location_ecef
            ant_names[i]   = np.string_(cminfo["antenna_names"][n])
            ant_nums[i]    = cminfo["antenna_numbers"][n]
            ant_pos_enu[i] = cminfo["antenna_positions_enu"][n]
        for i,(a,b) in enumerate(baselines):
            uvw[i] = ant_pos_enu[a] - ant_pos_enu[b]
        header.create_dataset("antenna_names",     dtype="|S5", shape=(NANTS,), data=ant_names)
        header.create_dataset("antenna_numbers",   dtype="<i8", shape=(NANTS,), data=ant_nums)
        header.create_dataset("antenna_positions",   dtype="<f8", shape=(NANTS,3), data=ant_pos)
        header.create_dataset("antenna_positions_enu",   dtype="<f8", shape=(NANTS,3), data=ant_pos_enu)
        header.create_dataset("latitude",    dtype="<f8", data=cminfo["cofa_lat"])
        header.create_dataset("longitude",   dtype="<f8", data=cminfo["cofa_lon"])
    else:
        header.create_dataset("altitude",    dtype="<f8", data=0.0)
        header.create_dataset("antenna_names",     dtype="|S5", shape=(NANTS,), data=["NONE"]*NANTS)
        header.create_dataset("antenna_numbers",   dtype="<i8", shape=(NANTS,), data=range(NANTS))
        header.create_dataset("antenna_positions",   dtype="<f8", shape=(NANTS,3), data=np.zeros([NANTS,3]))
        header.create_dataset("antenna_positions_enu",   dtype="<f8", shape=(NANTS,3), data=np.zeros([NANTS,3]))
        header.create_dataset("latitude",    dtype="<f8", data=0.0)
        header.create_dataset("longitude",   dtype="<f8", data=0.0)

    # lst_array needs populating by receiver. Should be center of integrations in radians
    header.create_dataset("lst_array",   dtype="<f8", data=np.zeros(n_bls))
    # time_array needs populating by receiver (should be center of integrations in JD)
    #header.create_dataset("time_array", dtype="<f8", data=np.zeros(n_bls * NTIMES))
    # uvw_needs populating by receiver: uvw = xyz(ant2) - xyz(ant1). Units, metres.
    header.create_dataset("uvw_array",  dtype="<f8", data=uvw)
    # !Some! extra_keywords need to be computed for each file
    add_extra_keywords(header, cminfo, fenginfo)


def add_extra_keywords(obj, cminfo=None, fenginfo=None):
    extras = obj.create_group("extra_keywords")
    if cminfo is not None:
        extras.create_dataset("cmver", data=np.string_(cminfo["cm_version"]))
        # Convert any numpy arrays to lists so they can be JSON encoded
        cminfo_copy = copy.deepcopy(cminfo)
        for key in cminfo_copy.keys():
            if isinstance(cminfo_copy[key], np.ndarray):
                cminfo_copy[key] = cminfo_copy[key].tolist()
        extras.create_dataset("cminfo", data=np.string_(json.dumps(cminfo_copy)))
        del(cminfo_copy)
    else:
        extras.create_dataset("cmver", data=np.string_("generated-without-cminfo"))
        extras.create_dataset("cminfo", data=np.string_("generated-without-cminfo"))
    if fenginfo is not None:
        extras.create_dataset("finfo", data=np.string_(json.dumps(fenginfo)))
    else:
        extras.create_dataset("finfo", data=np.string_("generated-without-redis"))
    #extras.create_dataset("st_type", data=np.string_("???"))
    extras.create_dataset("duration", dtype="<f8", data=0.0) # filled in by receiver
    extras.create_dataset("obs_id", dtype="<i8", data=0)     # filled in by receiver
    extras.create_dataset("startt", dtype="<f8", data=0.0)   # filled in by receiver
    extras.create_dataset("stopt",  dtype="<f8", data=0.0)   # filled in by receiver
    extras.create_dataset("corr_ver",  dtype="|S32", data=np.string_("unknown"))# filled in by receiver
    extras.create_dataset("tag",  dtype="|S128", data=np.string_("unknown"))# filled in by receiver

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Create a template HDF5 header file, optionally '\
                                     'using the correlator C+M system to get current meta-data',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('output', type=str, help = 'Path to which the template header file should be output')
    parser.add_argument('-c', dest='use_cminfo', action='store_true', default=False,
                        help ='Use this flag to get up-to-date (hopefully) array meta-data from the C+M system')
    parser.add_argument('-r', dest='use_redis', action='store_true', default=False,
                        help ='Use this flag to get up-to-date (hopefully) f-engine meta-data from a redis server at `redishost`')
    parser.add_argument('--config', type=str, default='/tmp/bdaconfig.txt', 
                        help = 'BDA Configuration file to create header (taken from redis by default)')
    args = parser.parse_args()

    if args.config:
       config = args.config

    with h5py.File(args.output, "w") as h5:
        create_header(h5, config, use_cm=args.use_cminfo, use_redis=args.use_redis)
