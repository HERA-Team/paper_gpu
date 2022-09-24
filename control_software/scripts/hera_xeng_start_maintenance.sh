#! /bin/bash

# source conda environment
source ~/hera-venv/bin/activate hera

LOGFILE=~/xeng_start.log

Help()
{
   echo "Options:"
   echo "o     Observation length in hours. Default 2"
   echo "t     Observing tag. Default 'engineering'"
   echo "h     This help"
   echo
}

# Default values
obslen="2"
TAG=engineering

# Process input arguments
while getopts "ho:t:" option; do
    case $option in
      h) # display Help
         Help
         exit;;
      o) # obslen
         obslen="$OPTARG"
         ;;
      t) # tag
         TAG="$OPTARG"
         ;;
     \?) # Invalid option
         Help
         exit;;
    esac
done

# Kill off anything still running
hera_xeng_stop.sh

# Start afresh
echo Starting X-Engines > $LOGFILE
date >> $LOGFILE
echo xtor_up.py --runtweak --redislog px{1..16} &>> $LOGFILE
xtor_up.py --runtweak --redislog px{1..16} &>> $LOGFILE
echo hera_catcher_up.py --redislog &>> $LOGFILE
hera_catcher_up.py --redislog &>> $LOGFILE
echo hera_catcher_ctl.py start --obslen=$obslen --tag $TAG &>> $LOGFILE
hera_catcher_ctl.py start --obslen=$obslen --tag $TAG &>> $LOGFILE
