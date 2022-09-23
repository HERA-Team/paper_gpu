#!/usr/bin/env python

import redis
import argparse
from paper_gpu import catcher

parser = argparse.ArgumentParser(
    description='Turn HERA correlator on/off at next LST bin boundary',
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('action',type=str,
                    help='Action: "start"|"stop" the correlator.')
parser.add_argument('-r', dest='redishost', type=str, default='redishost',
                    help='Hostname of redis server')
# XXX remove obslen and just record until stop is issued
parser.add_argument('--obslen', type=float, default=1,
                    help='Observation length in hours. default 1')
parser.add_argument('--tag', dest='tag', type=str, default='delete',
                    help='A descriptive tag to go into data files')
args = parser.parse_args()

assert args.action in ['start', 'stop'], 'Available actions are "start" and "stop"'

if args.action == 'stop':
    catcher.stop_observing(redishost=args.redishost)

if args.action == 'start':
    rdb = redis.Redis(args.redishost)
    feng_sync_time_ms = int(rdb['corr:feng_sync_time'])
    prms = catcher.set_observation(args.obslen, feng_sync_time_ms,
                                   redishost=args.redishost)
    catcher.set_xeng_output_redis_keys(prms['trig_mcnt'],
                                       prms['acclen'],
                                       redishost=args.redishost)
    catcher.start_observing(args.tag, prms['ms_per_file'], prms['nfiles'],
                            redishost=args.redishost)
