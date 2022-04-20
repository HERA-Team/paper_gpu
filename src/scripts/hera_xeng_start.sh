#! /bin/bash

# set PATH explicitly
export PATH="/usr/local/bin:/usr/bin:/bin"

# source conda environment
source ~/hera-venv/bin/activate

LOGFILE=~/xeng_start.log
export TAG=science
export CATCHERHOST=hera-sn1

# Kill off anything still running
hera_xeng_stop.sh

# Start afresh
echo Starting X-Engines > $LOGFILE
date >> $LOGFILE
xtor_up.py --runtweak --redislog px{1..16} &>> $LOGFILE
hera_catcher_up.py --redislog $CATCHERHOST &>> $LOGFILE

# XXX add arguments to set_observation
hera_set_observation.py --obslen=10 &>> $LOGFILE
hera_ctl.py start &>> $LOGFILE
hera_catcher_take_data.py --tag $TAG $CATCHERHOST &>> $LOGFILE
