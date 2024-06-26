#! /bin/bash

# source conda environment
source ~/miniforge3/bin/activate hera

LOGFILE=~/xeng_start.log
export TAG="${1:-science}"
echo "using tag $TAG"

# Kill off anything still running
hera_xeng_stop.sh

# Start afresh
echo Starting X-Engines > $LOGFILE
date >> $LOGFILE
xtor_up.py --runtweak --redislog px{1..16} &>> $LOGFILE
hera_catcher_up.py --redislog &>> $LOGFILE

# XXX add arguments to set_observation
hera_catcher_ctl.py start --tag $TAG &>> $LOGFILE
