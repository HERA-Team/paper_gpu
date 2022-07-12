#!/usr/bin/env python

"""BDA correlator."""

import json
import logging
import numpy as np
import redis
import yaml
from hera_corr_cm.handlers import add_default_log_handlers

from paper_gpu import bda

logger = add_default_log_handlers(logging.getLogger(__file__))


def get_corr_to_hera_map(r, nants_data=192, nants=352, verbose=False):
    """
    Return the correlator map.

    Parameters
    ----------
    r : redis.Redis object
        The redis instance to fetch data from.
    nants_data : int, optional
        The number of antennas reporting data. This is the maximum range of
        antenna numbers in the correlator input mapping.
    nants : int, optional
        The total number of antennas in the array. This is the maximum number of
        antennas in HERA.
    verbose : bool, optional
        Whether to echo out the corr -> HERA mapping.

    Returns
    -------
    out_map : ndarray of ints
        The mapping between correlator input and HERA antenna number. The
        integer saved at a particular index `i` is the HERA ant number that
        corresponds to correlator input `i`.
    """
    out_map = np.arange(nants, nants + nants_data)  # use default values outside the range of real antennas

    # A dictionary with keys which are antenna numbers
    # of the for {<ant> :{<pol>: {'host':SNAPHOSTNAME, 'channel':INTEGER}}}
    ant_to_snap = json.loads(r.hget("corr:map", "ant_to_snap"))
    config = yaml.safe_load(r.hget("snap_configuration", "config"))

    for ant, pol in ant_to_snap.items():
        hera_ant_number = int(ant)
        try:
            pol_key = list(pol.keys())[0]
            host = pol[pol_key]["host"]
            chan = pol[pol_key]["channel"]  # runs 0-5
            snap_ant_chans = str(config['fengines'][host]['ants'])
        except(KeyError):
            snap_ant_chans = None
        if snap_ant_chans is None:
            logger.warning("Couldn't find antenna indices for %s" % host)
            continue
        corr_ant_number = json.loads(snap_ant_chans)[chan//2] #Indexes from 0-3 (ignores pol)
        out_map[corr_ant_number] = hera_ant_number
        if verbose:
            print(corr_ant_number)
            print(
                "HERA antenna %d maps to correlator input %d" % (
                    hera_ant_number, corr_ant_number
                )
            )

    return out_map

def populate_redis_data(redishost="redishost", verbose=False):
    """
    Populate redis with important correlator information.

    Parameters
    ----------
    redishost : str, optional
        The hostname of the redis server.

    verbose : bool, optional
        Whether to echo the mapping to the user.

    Returns
    -------
    None
    """
    # get config data
    config = bda.read_bda_config_from_redis(redishost)

    # fetch other data from redis
    r = redis.Redis("redishost")
    corr_to_hera_map = get_corr_to_hera_map(
        r, nants_data=192, nants=192, verbose=verbose
    )

    # populate integration bin list
    integration_bin = []
    for i, t in enumerate(config[:, 2]):
        if (t != 0):
           integration_bin.append(np.repeat(t, int(8 // t)))
    integration_bin = np.asarray(np.concatenate(integration_bin), dtype=np.float64)

    # save into redis
    # we save as one long string, with newlines to differentiate
    corr_to_hera_map_str = "\n".join([str(ant) for ant in list(corr_to_hera_map)])
    r.hset("corr", "corr_to_hera_map", corr_to_hera_map_str)

    integration_bin_str = "\n".join([str(bl) for bl in list(integration_bin)])
    r.hset("corr", "integration_bin", integration_bin_str)

    return

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Create a template HDF5 header file, optionally using the correlator "
            "C+M system to get current meta-data"
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--redishost", default="redishost", help ="Redis host name.")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    populate_redis_data(args.redishost, args.verbose)
