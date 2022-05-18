#!/usr/bin/env python

from paper_gpu import bda
import argparse

NANTS = 352

parser = argparse.ArgumentParser(description='Create a configuration file for BDA '\
                                 'using the correlator C+M system to get current meta-data'\
                                 'NO BDA IS CURRENTLY PERFORMED!',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-c', dest='use_cminfo', action='store_true', default=False,
                    help ='Use this flag to get up-to-date (hopefully) array meta-data from the C+M system')
parser.add_argument('--redishost', default='redishost',
                    help ='Redis host. Default: redishost')
parser.add_argument('-r', dest='use_redis', action='store_true', default=False,
                    help ='Use this flag to get up-to-date (hopefully) f-engine meta-data from a redis server at `redishost`')
parser.add_argument('-n', dest='n_ants_data', type=int, default=192,
                    help ='Number of antennas that have data (used if cminfo is not set)')
args = parser.parse_args()

bl_pairs = bda.create_bda_config(args.n_ants_data, use_cm=args.use_cminfo, use_redis=args.use_redis, nants=NANTS)
bda.write_bda_config_to_redis(bl_pairs, redishost=args.redishost)
