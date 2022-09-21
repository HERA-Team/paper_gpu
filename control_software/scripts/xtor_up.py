#!/usr/bin/env python

import os
import redis
import time
import argparse
import subprocess
from paper_gpu import bda
from paper_gpu.utils import run_on_hosts

NANTS = 352


parser = argparse.ArgumentParser(description='Start the HERA X-engines',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)

parser.add_argument('hosts', type=str, nargs='+', help='Hosts to intialize')
parser.add_argument('-r', dest='redishost', type=str, default='redishost', help='Host serving redis database')
parser.add_argument('-t', dest='timeslices', type=int, default=2,
                    help='Number of independent correlators. E.g. 2 => Even/odd correlator')
parser.add_argument('-i', dest='ninstances', type=int, default=2,
                    help='Number of pipeline instances per host')
parser.add_argument('--runtweak', dest='runtweak', action='store_true', default=False,
                    help='Run tweak-perf.sh on X-hosts before starting correlator')
parser.add_argument('--ibverbs', dest='ibverbs', action='store_true', default=False,
                    help='Use the IB Verbs netthread. Experimental!')
parser.add_argument('--redislog', dest='redislog', action='store_true', default=False,
                    help='Use the redis logger to duplicate log messages on redishost\'s log-channel pubsub stream')
parser.add_argument('--nodatabase', dest='nodatabase', action='store_true', default=False,
                    help='Don\'t try to get configuration from the site database.')
parser.add_argument('--test', dest='test', action='store_true', default=False,
                    help='Run BDA in test vector mode')
parser.add_argument('-n', dest='n_ants_data', type=int, default=192,
                    help ='Number of antennas that have data (used if cminfo is not set)')
parser.add_argument('--pypath', dest='pypath', type=str, default="/home/hera/hera-venv",
                    help='The path to a python virtual environment which will be activated prior to running paper_init. ' +
                         'Only relevant if using the --redislog flag, which uses a python redis interface')

args = parser.parse_args()
hosts = args.hosts
python_source_cmd = ["source", os.path.join(args.pypath, "bin/activate"), "hera", ";"]
nhosts = len(hosts)
nhosts_per_timeslice = nhosts // args.timeslices

assert args.ninstances == 2, 'Sorry, anything other than ninstances=2 is not supported!'

r = redis.Redis(args.redishost)

# Run performance tweaking script
if args.runtweak:
    run_on_hosts(hosts, 'tweak-perf.sh', user='root', wait=True)

# Start X-Engines
init_args = []
if args.ibverbs:
    init_args += ['-i']
if args.redislog:
    init_args += ['-r']
if args.test:
    init_args += ['-t']

if args.redislog:
    # two instances per host
    run_on_hosts(hosts, python_source_cmd + ['paper_init.sh'] + init_args + ['0','1'], wait=True)
elif args.test:
    # two instances per host
    run_on_hosts(hosts, ['paper_init.sh'] + init_args + ['0'], wait=True)
else:
    # two instances per host
    run_on_hosts(hosts, ['paper_init.sh'] + init_args + ['0', '1'], wait=True)

# Start hashpipe<->redis gateways
cpu_masks = ['0x0080', '0x8000']
for host in hosts:
    for i in range(args.ninstances):
        run_on_hosts([host], ['taskset', cpu_masks[i], 'hashpipe_redis_gateway.rb', '-g', host, '-i', '%d' % i])

# Wait for the gateways to come up
time.sleep(3)

# Generate the BDA config file and upload to redis
bl_pairs = bda.create_bda_config(args.n_ants_data, nants=NANTS)
bda.write_bda_config_to_redis(bl_pairs, redishost=args.redishost)

# publish to hashpipe
for host in hosts:
    for i in range(args.ninstances):
        key = "hashpipe://%s/%d/set" % (host, i)
        r.publish(key, "BDACONF=set")

# Configure the X-engines as even/odd correlators
if (len(hosts) == 1) and (args.timeslices != 1):
   for i in range(args.ninstances):
      key = 'hashpipe://%s/%d/set' % (host, i)
      r.publish(key, 'TIMEIDX=%d' % (i))
else: 
   for hn, host in enumerate(hosts):
      for i in range(args.ninstances):
         key = 'hashpipe://%s/%d/set' % (host, i)
         r.publish(key, 'TIMEIDX=%d' % (hn // nhosts_per_timeslice))

time.sleep(2)

# Let the network threads begin processing
for hn, host in enumerate(hosts):
   for i in range(args.ninstances):
      key = 'hashpipe://%s/%d/set' % (host, i)
      r.publish(key, 'NETHOLD=0')

time.sleep(2)

# Reset various statistics counters
for hn, host in enumerate(hosts):
   for i in range(args.ninstances):
      key = 'hashpipe://%s/%d/set' % (host, i)
      for v in ['NETWAT', 'NETREC', 'NETPRC']:
          r.publish(key, '%sMN=99999' % (v))
          r.publish(key, '%sMX=0' % (v))
