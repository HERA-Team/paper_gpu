#!/home/hera/hera-venv/bin/python
# -*- coding: utf-8 -*-

import redis
from astropy.time import Time, TimeDelta
from astropy import units
from hera_mc.utils import LSTScheduler

REDISHOST = 'redishost'

r = redis.Redis(REDISHOST, decode_responses=True)

# observation length in seconds
OBSLEN = 7200  # 2 hours

# set a start time for the catcher for 1 minute from now
acclen = 147456
acclen_calc = acclen // 4 # XXX figure out where magic 4 comes from

X_PIPES = 2
file_duration_ms = int(2 * 2 * (acclen_calc * 2) * X_PIPES * 2 * 8192 / 500e6 * 1000)
file_duration_s = file_duration_ms / 1000
t0 = Time.now() + TimeDelta(1 * units.min)
lst_time = LSTScheduler(t0, file_duration_s)
starttime = lst_time[0].unix * 1000

r.set('corr:acc_len', str(acclen_calc))
r.set('corr:start_time', str(starttime))
r.set('corr:obs_len', str(OBSLEN))

print("Set corr:acc_len, corr:start_time, corr:obs_len")
