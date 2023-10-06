#! /bin/bash

LOGFILE=~/catcher_watchdog.log
RUNNING=`ssh hera-sn1 "ps aux | grep [h]era_catcher_net_thread"`
date >> $LOGFILE
echo "Result: ${RUNNING}" >> $LOGFILE
if [[ -n ${RUNNING} ]]; then
    echo "Catcher running" >> $LOGFILE
else
    echo "Catcher DOWN. Restarting..." >> $LOGFILE
    #hera_catcher_ctl.py stop
    #hera_catcher_down.sh
    #hera_catcher_up.py --redislog
    #hera_catcher_ctl.py start --tag science
fi


