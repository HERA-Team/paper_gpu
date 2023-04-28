#!/bin/bash

# Add directory containing this script to PATH
PATH="$(dirname $0):${PATH}"

# Activate conda env
source ~/hera-venv/bin/activate hera

# enable gdb debugging of segfauls
ulimit -c unlimited

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
  # This is a coarse mask that selects cores 6-11.  Hera-sn1 is a dual socket
  # system with 6 cores per socket and the NIC is attached to the second socket
  # (i.e. cores 6-11).  We more finely control thread affinity below.
  mask=0x00fc0
  bindhost=eth4
  # Generally we avoid the first core of each socket, so for hera-sn1 we would
  # avoid core 6 in the second socket, but we have more cores than threads so
  # we basically work backwards from 11.
  netcpu=9
  ibvcpu=10
  autocpu=11
  outmask=0x180 # cores 7 and 8

  echo "Using BDA threads"
  echo taskset $mask \
  hashpipe -p paper_gpu -I $instance \
    -o BINDHOST=$bindhost \
    -o IBVPKTSZ=42,24,4096 \
    -c $ibvcpu ibvpkt_thread \
    -c $netcpu hera_catcher_ibvpkt_thread \
    -m $outmask hera_catcher_disk_thread \
    -c $autocpu hera_catcher_autocorr_thread

  if [ $USE_REDIS -eq 1 ]
  then
    echo "Using redis logger"
    { taskset $mask \
    hashpipe -p paper_gpu -I $instance \
      -o BINDHOST=$bindhost \
      -o IBVPKTSZ=42,24,4096 \
      -c $ibvcpu ibvpkt_thread \
      -c $netcpu hera_catcher_ibvpkt_thread \
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
      -o IBVPKTSZ=42,24,4096 \
      -c $ibvcpu ibvpkt_thread \
      -c $netcpu hera_catcher_ibvpkt_thread \
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
