#!/home/hera/hera-venv/bin/python
# -*- coding: utf-8 -*-

import sys
from astropy.time import Time, TimeDelta
from astropy import units
from hera_mc.utils import LSTScheduler
from hera_corr_cm.hera_corr_handler import HeraCorrHandler

# observation length in seconds
OBSLEN = 7200  # 2 hours

# restart correlator
hch = HeraCorrHandler()
# _xtor_down() doesn't return anything
hch._xtor_down()
status = hch._xtor_up()
if status is not True:
    print("Correlator failed to start properly")
    sys.exit(1)

# set a start time for the catcher for 1 minute from now
acclen = 147456
acclen_calc = acclen // 4
X_PIPES = 2
file_duration_ms = int(2 * 2 * (acclen_calc * 2) * X_PIPES * 2 * 8192 / 500e6 * 1000)
file_duration_s = file_duration_ms / 1000
t0 = Time.now() + TimeDelta(1 * units.min)
lst_time = LSTScheduler(t0, file_duration_s)
starttime = lst_time[0].unix * 1000

# _start_capture() doesn't return anything


# Adding noise obs - DCJ 2021
# Doesn't work, not actually supported by redis-cmd  - DCJ 
#print("start_correlator.py :  switching all antennas to LOAD state",flush=True)
##NOISE = LOAD because of issue 677
#hch.cm.noise_diode_enable()#Defaults to switching all antennnas. 
#print("start_correlator.py : LOAD switch complete",flush=True)
## ---- noise -----

print("start_correlator.py : starting observing at",flush=True)
print(starttime, acclen)
hch._start_capture(starttime, OBSLEN, acclen, "engineering")

print("Successfully started correlator")
