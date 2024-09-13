# -*- coding: utf-8 -*-
# Copyright (c) 2022 The HERA Collaboration
# Licensed under the 2-clause BSD License

import redis
import json
import yaml
import argparse
import numpy as np
from .file_conversion import get_antpos_info

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
        corr_nums.append((a, corr_ant_number))
    return corr_nums


def create_bda_config(n_ants_data, nants=352):
    cminfo = get_cm_info()
    r = redis.Redis('redishost', decode_responses=True)
    corr_map = r.hgetall("corr:map")
    config = yaml.safe_load(r.hget("snap_configuration", "config"))
    field2corr_nums = get_hera_to_corr_ants(corr_map, config)
    xyz, ants = get_antpos_info()
    antpos = dict(zip(map(lambda x: int(x[2:]), ants), xyz))
    bl_pairs = assign_bl_pair_tier(field2corr_nums, antpos, nants=nants)
    return bl_pairs


def assign_bl_pair_tier(field2corr_nums, antpos, cen_ants=(145, 165, 166),
                        edge_ant=155, tol=1, nants=352):
    """
    Assign a BDA tier to each baseline pair.

    Parameters
    ----------
    corr_nums : list
        The list of correlator inputs
    """
    cen = np.mean(np.array([antpos[ca] for ca in cen_ants]), axis=0)
    antdist = {k: np.linalg.norm(v - cen) for k, v in antpos.items()}
    outriggers = set(k for k, v in antdist.items() if v > antdist[edge_ant] + tol)
    corr2ant = {c: f for f, c in field2corr_nums}
    bl_pairs = []

    for corr0 in range(nants):
        ant0 = corr2ant.get(corr0, -1)
        for corr1 in range(corr0, nants):
            ant1 = corr2ant.get(corr1, -1)
            if ant0 < 0 or ant1 < 0:
                # Tier 0 is not recorded
                bl_pairs.append([corr0, corr1, 0])
            elif ant0 in outriggers or ant1 in outriggers:
                bl_pairs.append([corr0, corr1, 2])
            else:
                bl_pairs.append([corr0, corr1, 4])
    return bl_pairs


def write_bda_config_to_redis(bl_pairs, redishost="localhost"):
    # convert list of lists to single string
    bl_pairs_list = [" ".join(map(str, blp)) for blp in bl_pairs]
    bl_pairs_str = "\n".join(bl_pairs_list)

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
