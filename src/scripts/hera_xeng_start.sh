#! /bin/bash

source ~/.bashrc
set -e # exit with an error if any subcommand returns an error; .bashrc has such a command!

LOGFILE=~/xeng_start.log
ERRFILE=~/xeng_start.err
export TAG=engineering
export CATCHERHOST=hera-sn1

# Kill off anything still running
hera_xeng_stop.sh

# Start afresh
date > $LOGFILE
echo > $ERRFILE
xtor_up.py --runtweak --redislog px{1..16} >> $LOGFILE 2>> $ERRFILE
hera_catcher_up.py --redislog $CATCHERHOST >> $LOGFILE 2>> $ERRFILE

# XXX add arguments to set_observation
hera_set_observation.py >> $LOGFILE 2>> $ERRFILE
hera_ctl.py start >> $LOGFILE 2>> $ERRFILE
hera_catcher_take_data.py --tag $TAG $CATCHERHOST >> $LOGFILE 2>> $ERRFILE
