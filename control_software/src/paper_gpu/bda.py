# -*- coding: utf-8 -*-
# Copyright (c) 2022 The HERA Collaboration
# Licensed under the 2-clause BSD License

import redis
import json
import yaml
import argparse

from hera_corr_cm import redis_cm

def get_cm_info():
    """Return cm_info as if from hera_mc."""
    return redis_cm.read_cminfo_from_redis(return_as='dict')


# might be able to replace most of this with code from hera_corr_cm
def get_hera_to_corr_ants(corr_map, snap_config):
    """
    Get the mapping between HERA antenna number and correlator input.

    Parameters
    ----------
    corr_map : dict
        A dictionary retrieved from redis containing the `ant_to_snap` key.
    snap_config : dict
        The SNAP configuration as extracted from the YAML file.

    Returns
    -------
    corr_nums : list of int
        The correlator input indices corresponding to HERA antenna numbers.
    """
    ant_to_snap = json.loads(corr_map["ant_to_snap"])
    ants = [int(a) for a in ant_to_snap.keys()]
    corr_nums = []
    for a in ants:
        host = ant_to_snap['%d'%a]['n']['host']
        chan = ant_to_snap['%d'%a]['n']['channel'] # snap_input_number
        try:
            snap_ant_chans = str(snap_config['fengines'][host]['ants'])
        except(KeyError):
            continue
        corr_ant_number = json.loads(snap_ant_chans)[chan//2] #Indexes from 0-3 (ignores pol)
        corr_nums.append(corr_ant_number)
    return corr_nums


def create_bda_config(n_ants_data, use_cm=False, use_redis=False):
    cminfo = get_cm_info()

    r = redis.Redis('redishost', decode_responses=True)
    corr_map = json.loads(r.hgetall("corr:map"))
    config = yaml.safeload(r.hget("snap_configuration", "config"))
    corr_ant_nums = get_hera_to_corr_ants(corr_map, config)
    bl_pairs = assign_bl_pair_tier(corr_ant_nums)
    return bl_pairs


def assign_bl_pair_tier(corr_nums, nants=352):
    """
    Assign a BDA tier to each baseline pair.

    Parameters
    ----------
    corr_nums : list
        The list of correlator inputs
    """
    bl_pairs = []
    #for ant0 in corr_nums
    for ant0 in range(nants):
        for ant1 in range(ant0, nants, 1):
            if (ant0 in corr_nums) and (ant1 in corr_nums):
               bl_pairs.append([ant0, ant1, 4])
            else:
               bl_pairs.append([ant0, ant1, 0])

    return bl_pairs


def write_bda_config_to_redis(bl_pairs, redishost="localhost"):
    # convert list of lists to single string
    bl_pairs_list = [" ".join(map(str, blp)) for blp in bl_pairs]
    bl_pairs_str = "\n".join(bl_pairs_list)
    print("length: ", len(bl_pairs_str))

    # Write baseline-pair data to redis
    with redis.Redis(redishost, decode_responses=True) as rclient:
        rclient.hset("corr", "bl_bda_tiers", bl_pairs_str)

    return


def read_bda_config_from_redis(redishost="localhost"):
    # read in bl pair distribution from redis
    with redis.Redis(redishost, decode_responses=True) as rclient:
        bl_pairs_str = rclient.hget("corr", "bl_bda_tiers")

    # convert from string -> list of lists
    bl_pairs_list = bl_pairs_str.split("\n")
    bl_pairs = [list(map(int, blp.split(" "))) for blp in bl_pairs_list]

    return bl_pairs
