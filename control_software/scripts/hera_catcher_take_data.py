#!/usr/bin/env python

import redis
import time
import argparse
import subprocess

python_source_cmd = ['source', '~/hera-venv/bin/activate', 'hera']
redis_metadata_cmd = ["hera_init_catcher_data.py"]

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

parser = argparse.ArgumentParser(description='Trigger data collection on the HERA catcher node',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)

parser.add_argument('host', type=str, help='Host on which to capture data')
parser.add_argument('-r', dest='redishost', type=str, default='redishost', help='Host serving redis database')
parser.add_argument('--tag', dest='tag', type=str, default='none', help='A descriptive tag to go into data files')

args = parser.parse_args()

r = redis.Redis(args.redishost, decode_responses=True)
acclen = int(r.get('corr:acc_len'))
# XXX get these other variables from redis too
# XXX maybe move whole thing to hera_set_observation.py
XPIPES = 2
msperfile = int(2 * 2 * (acclen * 2) * XPIPES * 2 * 8192 / 500e6 * 1000)
obslen = int(float(r.get('corr:obs_len'))) # float cast for more robust string conversion
nfiles = int(1000 * obslen / msperfile)

if len(args.tag) > 127:
  raise ValueError("Tag argument must be <127 characters!")

# Populate redis with the necessary metadata
run_on_hosts([args.host], python_source_cmd + [";"] + redis_metadata_cmd + ["--verbose"], wait=True)

#Configure runtime parameters
catcher_dict = {
  'HDF5TPLT' : args.hdf5template,
  'MSPERFIL' : msperfile,
  'NFILES'   : nfiles,
  'SYNCTIME' : r['corr:feng_sync_time'],
  'INTTIME'  : r['corr:acc_len'],
  'TAG'      : args.tag,
}

pubchan = 'hashpipe://%s/%d/set' % (args.host, 0)
for key, val in catcher_dict.items():
   r.publish(pubchan, '%s=%s' % (key, val))

# Only trigger after the other parameters have had ample time to write
time.sleep(0.1)
r.publish(pubchan, "TRIGGER=1")
