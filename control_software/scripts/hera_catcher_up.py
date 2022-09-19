#!/usr/bin/env python

import redis
import time
import argparse
import subprocess
import numpy as np
import os
from paper_gpu import bda

perf_tweaker = 'tweak-perf-sn.sh'
init = 'hera_catcher_init.sh'

def run_on_hosts(hosts, cmd, user=None, wait=True):
    if isinstance(cmd, str):
        cmd = [cmd]
    p = []
    for host in hosts:
        if user is None:
            p += [subprocess.Popen(['ssh', '%s' % (host)] + cmd)]
        else:
            p += [subprocess.Popen(['ssh', '%s@%s' % (user, host)] + cmd)]
    if wait:
        for pn in p:
            pn.wait()

parser = argparse.ArgumentParser(description='Start the HERA Catcher Machine',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)

parser.add_argument('host', type=str, help='Host to intialize')
parser.add_argument('-r', dest='redishost', type=str, default='redishost', help='Host serving redis database')
parser.add_argument('--nobda', dest='nobda', action='store_true', default=False,
                    help='Use the baseline dependent averaging version')
parser.add_argument('--runtweak', dest='runtweak', action='store_true', default=False,
                    help='Run the tweaking script %s on X-hosts prior to starting the correlator' % perf_tweaker)
parser.add_argument('--redislog', dest='redislog', action='store_true', default=False,
                    help='Use the redis logger to duplicate log messages on redishost\'s log-channel pubsub stream')
parser.add_argument('--pypath', dest='pypath', type=str, default="/home/hera/hera-venv",
                    help='The path to a python virtual environment which will be activated prior to running paper_init. ' +
                         'Only relevant if using the --redislog flag, which uses a python redis interface')

args = parser.parse_args()

# Environment sourcing command required to run remote python jobs
python_source_cmd = ["source", os.path.join(args.pypath, "bin/activate"), "hera", ";"]

r = redis.Redis(args.redishost, decode_responses=True)

# Run performance tweaking script
if args.runtweak:
    run_on_hosts([args.host], perf_tweaker, user='root', wait=True)

init_args = []
if not args.nobda:
   init_args += ['-a']
if args.redislog:
   init_args += ['-r']

# Start Catcher
run_on_hosts([args.host], python_source_cmd + ['cd', '/data;', init] + init_args + ['0'], wait=True)
time.sleep(15)

# Start hashpipe<->redis gateways
cpu_mask = '0x0004'
run_on_hosts([args.host], ['taskset', cpu_mask, 'hashpipe_redis_gateway.rb', '-g', args.host, '-i', '0'])

# Wait for the gateways to come up
time.sleep(10)

# Generate the meta-data template
run_on_hosts([args.host], python_source_cmd + ["hera_init_catcher_data.py"] + ["--verbose"], wait=True)

#Configure runtime parameters
catcher_dict = {
  'NFILES'   : 0,
  'SYNCTIME' : r['corr:feng_sync_time'],
  'INTTIME'  : r['corr:acc_len'],
  'TRIGGER'  : 0,
}

# Reset various statistics counters

pubchan = 'hashpipe://%s/%d/set' % (args.host, 0)
for key, val in catcher_dict.items():
    r.publish(pubchan, '%s=%s' % (key, val))
for v in ['NETWAT', 'NETREC', 'NETPRC']:
    r.publish(pubchan, '%sMN=99999' % (v))
    r.publish(pubchan, '%sMX=0' % (v))
r.publish(pubchan, 'MISSEDPK=0')

# If BDA is requested, write distribution to redis
if not args.nobda:
    baselines = {}
    Nants = 0
    for n in range(4):
        baselines[n] = []

    bdaconfig = bda.read_bda_config_from_redis(redishost=args.redishost)
    for ant0, ant1, t in bdaconfig:
        if (t==0): continue
        n = int(np.log2(t))
        if (n==4): n = 3
        baselines[n].append((ant0, ant1))
        if(ant0 == ant1):
            Nants += 1

    for i in range(4):
        print((i, len(baselines[i])))
        r.publish(pubchan, 'NBL%dSEC=%d'  % (2**(i+1), len(baselines[i])))
    r.publish(pubchan, 'BDANANT=%d' % Nants)

    time.sleep(0.1)

# Release nethread hold
r.publish(pubchan, 'CNETHOLD=0')
