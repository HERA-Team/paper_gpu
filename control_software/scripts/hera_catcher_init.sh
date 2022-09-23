#!/bin/bash

# Add directory containing this script to PATH
PATH="$(dirname $0):${PATH}"

# Activate conda env
source ~/hera-venv/bin/activate hera

hostname=`hostname -s`

function getip() {
  out=$(host $1) && echo $out | awk '{print $NF}'
}

Help()
{
  echo "Usage: $(basename $0) [-a] INSTANCE_ID [...]"
  echo "  -r : Use redis logging (in addition to log files)"
}

myip=$(getip $(hostname))

function init() {
  instance=0
  mask=0x00fc0
  bindhost=eth4
  netcpu=9
  autocpu=11
  outmask=0x180

  echo "Using BDA threads"
  echo taskset $mask \
  hashpipe -p paper_gpu -I $instance \
    -o BINDHOST=$bindhost \
    -c $netcpu hera_catcher_net_thread \
    -m $outmask hera_catcher_disk_thread \
    -c $autocpu hera_catcher_autocorr_thread

  if [ $USE_REDIS -eq 1 ]
  then
    echo "Using redis logger"
    { taskset $mask \
    hashpipe -p paper_gpu -I $instance \
      -o BINDHOST=$bindhost \
      -c $netcpu hera_catcher_net_thread   \
      -m $outmask hera_catcher_disk_thread  \
      -c $autocpu hera_catcher_autocorr_thread \
    < /dev/null 2>&3 1>~/catcher.out.$instance; } \
    3>&1 1>&2 | tee ~/catcher.err.$instance | \
    stdin_to_redis.py -l WARNING > /dev/null &
  else
    echo "*NOT* using redis logger"
    taskset $mask \
    hashpipe -p paper_gpu -I $instance \
      -o BINDHOST=$bindhost \
      -c $netcpu hera_catcher_net_thread \
      -m $outmask hera_catcher_disk_thread \
      -c $autocpu hera_catcher_autocorr_thread \
       < /dev/null \
      1> ~/catcher.out.$instance \
      2> ~/catcher.err.$instance &
  fi
}

# Default to not using BDA version
USE_REDIS=0

for arg in $@; do
  case $arg in
    -h)
      Help
      exit 0
    ;;
    -r)
      USE_REDIS=1
      shift
    ;;
  esac
done

if [ -z "$1" ]
then
  Help
  exit 1
fi

for instidx in "$@"
do
  echo Starting instance catcher/$instidx
  init
  echo Instance catcher/$instidx pid $!
  # Sleep to let instance come up
  sleep 10
done

# Zero out MISSEDPK counts
for instidx in "$@"
do
  echo Resetting MISSEDPK counts for catcher/$instidx
  hashpipe_check_status -I $instidx -k MISSEDPK -s 0
done
