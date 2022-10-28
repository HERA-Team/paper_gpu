#!/usr/bin/env python

import redis
import argparse
from paper_gpu import catcher

parser = argparse.ArgumentParser(
    description='Turn HERA correlator on/off at next LST bin boundary',
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('action',type=str,
                    help='Action: "start"|"stop"|"endofday" the correlator.')
parser.add_argument('-r', dest='redishost', type=str, default='redishost',
                    help='Hostname of redis server')
parser.add_argument('--start_delay', type=float, default=30,
                    help='Seconds from present to start observing. Default 30')
parser.add_argument('--tag', dest='tag', type=str, default='delete',
                    help='A descriptive tag to go into data files')
args = parser.parse_args()

assert args.action in ['start', 'stop'], 'Available actions are "start" and "stop"'
assert args.tag in catcher.TAGS

if args.action == 'stop':
    catcher.stop_observing(redishost=args.redishost)
elif args.action == 'endofday':
    catcher.stop_observing(endofday=True, redishost=args.redishost)
elif args.action == 'start':
    rdb = redis.Redis(args.redishost)
    feng_sync_time_ms = int(rdb['corr:feng_sync_time'])
    prms = catcher.set_observation(feng_sync_time_ms, start_delay=args.start_delay,
                                   redishost=args.redishost)
    catcher.set_xeng_output_redis_keys(prms['trig_mcnt'],
                                       prms['acclen'],
                                       redishost=args.redishost)
    catcher.start_observing(args.tag, prms['ms_per_file'],
                            redishost=args.redishost)
