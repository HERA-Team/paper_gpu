#! /bin/bash

LOGFILE=~/xeng_start.log
ERRFILE=~/xeng_start.err
export TAG=engineering
export CATCHERHOST=hera-sn1

date > $LOGFILE
echo > $ERRFILE
xtor_up.py --runtweak --redislog px{1..16} >> $LOGFILE 2>> $ERRFILE
hera_catcher_up.py --redislog $CATCHERHOST >> $LOGFILE 2>> $ERRFILE

# XXX add arguments to set_observation
hera_set_observation.py >> $LOGFILE 2>> $ERRFILE
hera_ctl.py start >> $LOGFILE 2>> $ERRFILE
hera_catcher_take_data.py --tag $TAG $CATCHERHOST >> $LOGFILE 2>> $ERRFILE
