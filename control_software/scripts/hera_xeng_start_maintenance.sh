#! /bin/bash

# source conda environment
source ~/miniforge3/bin/activate hera

LOGFILE=~/xeng_start.log

Help()
{
   echo "Options:"
   echo "t     Observing tag. Default 'engineering'"
   echo "h     This help"
   echo
}

# Default values
TAG=engineering

# Process input arguments
while getopts "ht:" option; do
    case $option in
      h) # display Help
         Help
         exit;;
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
echo xtor_up.py --runtweak --redislog --noibverbs px{1..16} &>> $LOGFILE
xtor_up.py --runtweak --redislog --noibverbs px{1..16} &>> $LOGFILE
echo hera_catcher_up.py --redislog &>> $LOGFILE
hera_catcher_up.py --redislog &>> $LOGFILE
echo hera_catcher_ctl.py start --tag $TAG &>> $LOGFILE
hera_catcher_ctl.py start --tag $TAG &>> $LOGFILE
