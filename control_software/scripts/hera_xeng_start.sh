#! /bin/bash

# source conda environment
source ~/hera-venv/bin/activate hera

LOGFILE=~/xeng_start.log
export TAG=science

# Kill off anything still running
hera_xeng_stop.sh

# Start afresh
echo Starting X-Engines > $LOGFILE
date >> $LOGFILE
xtor_up.py --runtweak --redislog --noibverbs px{1..16} &>> $LOGFILE
hera_catcher_up.py --redislog &>> $LOGFILE

# XXX add arguments to set_observation
hera_catcher_ctl.py start --tag $TAG &>> $LOGFILE
