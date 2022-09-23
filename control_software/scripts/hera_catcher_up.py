#!/usr/bin/env python

import redis
import time
import argparse
import os
from paper_gpu import catcher
from paper_gpu.utils import run_on_hosts

parser = argparse.ArgumentParser(
    description='Start the HERA Catcher Machine',
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

parser.add_argument('--host', dest='host', type=str, default='hera-sn1',
    help='Catcher host (hera-sn1)')
parser.add_argument('-r', dest='redishost', type=str, default='redishost',
    help='Host serving redis database')
parser.add_argument('--runtweak', dest='runtweak',
    action='store_true', default=False,
    help='Run tweak-perf-sn.sh on host prior to start')
parser.add_argument('--redislog', dest='redislog',
    action='store_true', default=False,
    help="Use the redis logger to duplicate log messages on redishost's" +
         "log-channel pubsub stream")
parser.add_argument('--pypath', dest='pypath', type=str,
    default="/home/hera/hera-venv",
    help='The path to a python virtual environment which will be' +
         'activated prior to running paper_init. Only relevant if using' +
         ' the --redislog flag, which uses a python redis interface')

args = parser.parse_args()

# Environment sourcing command required to run remote python jobs
python_source_cmd = ["source", os.path.join(args.pypath, "bin/activate"), "hera", ";"]

r = redis.Redis(args.redishost, decode_responses=True)

# Run performance tweaking script
if args.runtweak:
    run_on_hosts([args.host], 'tweak-perf-sn.sh', user='root', wait=True)

init_args = []
if args.redislog:
   init_args += ['-r']

# Start Catcher
run_on_hosts([args.host], python_source_cmd + ['cd', '/data;', 'hera_catcher_init.sh'] + init_args + ['0'], wait=True)
#time.sleep(5)

# Start hashpipe<->redis gateways
cpu_mask = '0x0004'
procs = run_on_hosts([args.host], ['taskset', cpu_mask, 'hashpipe_redis_gateway.rb', '-g', args.host, '-i', '0']) # XXX should this wait?

# Wait for the gateways to come up
time.sleep(2)

catcher.clear_redis_keys(args.redishost)

# Release hold that was set by hera_catcher_net_thread
catcher.release_nethold(args.redishost)
