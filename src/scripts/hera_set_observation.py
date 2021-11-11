#!/home/hera/hera-venv/bin/python
# -*- coding: utf-8 -*-

import redis
from astropy.time import Time, TimeDelta
from astropy import units
from hera_mc.utils import LSTScheduler
import numpy as np

REDISHOST = 'redishost'

r = redis.Redis(REDISHOST, decode_responses=True)

# observation length in seconds
#OBSLEN = 7200  # 2 hours
OBSLEN = 12*3600 # 12 hours  11 Nov 2021 DCJ
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