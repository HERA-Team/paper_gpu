#!/home/hera/hera-venv/bin/python
# -*- coding: utf-8 -*-

import redis
from astropy.time import Time, TimeDelta
from astropy import units
from hera_mc.utils import LSTScheduler
import numpy as np


import argparse

parser = argparse.ArgumentParser(description='Schedule an observation at next available LST bin',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--obslen', type=float, default=1,
                    help ='Observation length in hours. default 1')
parser.add_argument('--redishost', dest='redishost',type=str, default='redishost',
                    help ='redis host. default=redishost')
args = parser.parse_args()


r = redis.Redis(args.redishost, decode_responses=True)

# observation length in seconds
OBSLEN = args.obslen*3600 
# set a start time for the catcher for 1 minute from now
acclen = 147456
acclen_calc = acclen // 4 # XXX figure out where magic 4 comes from

X_PIPES = 2
file_duration_ms = int(2 * 2 * (acclen_calc * 2) * X_PIPES * 2 * 8192 / 500e6 * 1000)
file_duration_s = file_duration_ms / 1000
t0 = Time.now() + TimeDelta(1 * units.min)
lst_time = LSTScheduler(t0, file_duration_s)
starttime = lst_time[0].unix * 1000 #start time in ms
starttime = int(np.round(starttime))

r.set('corr:acc_len', str(acclen_calc))
r.set('corr:start_time', str(starttime))
r.set('corr:obs_len', str(OBSLEN))

print("Set corr:acc_len, corr:start_time, corr:obs_len")
