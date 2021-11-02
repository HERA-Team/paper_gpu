#! /bin/bash

source ~/.bashrc
#set -e # exit with an error if any subcommand returns an error; .bashrc has such a command!

LOGFILE=~/xeng_start.log
export TAG=engineering
export CATCHERHOST=hera-sn1

# Kill off anything still running
/usr/local/bin/hera_xeng_stop.sh

# Start afresh
echo Starting X-Engines > $LOGFILE
date >> $LOGFILE
/usr/local/bin/xtor_up.py --runtweak --redislog px{1..16} &>> $LOGFILE
/usr/local/bin/hera_catcher_up.py --redislog $CATCHERHOST &>> $LOGFILE

# XXX add arguments to set_observation
/usr/local/bin/hera_set_observation.py &>> $LOGFILE
/usr/local/bin/hera_ctl.py start &>> $LOGFILE
/usr/local/bin/hera_catcher_take_data.py --tag $TAG $CATCHERHOST &>> $LOGFILE
