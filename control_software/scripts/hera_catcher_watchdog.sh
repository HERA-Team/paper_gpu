#! /bin/bash

# Activate HERA virtual environment
. ~/miniforge3/bin/activate hera

LOGFILE=~/catcher_watchdog.log
RUNNING=`ssh hera-sn1 "ps aux | grep [h]era_catcher_disk_thread"`
echo >> $LOGFILE
date >> $LOGFILE
echo "Result: ${RUNNING}" >> $LOGFILE
if [[ -n ${RUNNING} ]]; then
    echo "Catcher running" >> $LOGFILE
else
    echo "Catcher DOWN. Restarting..." >> $LOGFILE
    TS=$(date +%Y%m%dT%H%M)
    mv /home/hera/catcher.err.0 /home/hera/catcher.err.0.$TS
    mv /home/hera/catcher.out.0 /home/hera/catcher.out.0.$TS
    #hera_catcher_ctl.py stop
    #hera_catcher_down.sh
    #hera_catcher_up.py --redislog
    #hera_catcher_ctl.py start --tag science
    hera_xeng_start.sh
fi
