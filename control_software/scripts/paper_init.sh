#!/bin/bash

# Add directory containing this script to PATH
PATH="$(dirname $0):${PATH}"

# Activate conda env
source ~/miniforge3/bin/activate hera

hostname=`hostname -s`

hashpipe=hashpipe
plugin=paper_gpu.so
extraopts="-o IBVPKTSZ=42,8,4608"

function getip() {
  out=$(host $1) && echo $out | awk '{print $NF}'
}

myip=$(getip $(hostname))

# Determine which, if any, pxN alias maps to IP of current host.
# If a pxN match is found, mypx gets set to N (i.e. just the numeric part).
# If no match is found, mypx will be empty.
mypx=
for p in {1..16}
do
  ip=$(getip px${p})
  [ "${myip}" == "${ip}" ] || continue
  mypx=$p
done

# If no pxN alias maps to IP of current host, abort
if [ -z ${mypx} ]
then
  echo "$(hostname) is not aliased to a pxN name"
  exit 1
fi

case ${hostname} in

  snb*)
    # Calculate XIDs based on mypx
    xid0=$((  0 + (mypx-1) ))
    xid1=$((  8 + (mypx-1) ))
    xid2=$(( 16 + (mypx-1) ))
    xid3=$(( 24 + (mypx-1) ))

#    instances=(
#      # Setup parameters for four instances.
#      # 2 x E5-2660 (8-cores @ 2.2 GHz, 20 MB L3, 8.0 GT/s QPI, 1600 MHz DRAM)
#      # Fluff thread and output thread share a core.
#      # Save core  0 for OS.
#      # Save core  7 for eth2 and eth3
#      # Save core  8 for symmetry with core 0
#      # Save core 15 for eth4 and eth5
#      #
#      # Setup for four GPU devices (two GTX690s == two dual-GTX680s).
#      #
#      #                               GPU       NET FLF GPU OUT
#      # mask  bind_host               DEV  XID  CPU CPU CPU CPU
#      "0x007e ${hostname}-2.tenge.pvt  0  $xid0  1   2   3   2" # Instance 0, eth2
#      "0x007e ${hostname}-3.tenge.pvt  1  $xid1  4   5   6   5" # Instance 1, eth3
#      "0x7e00 ${hostname}-4.tenge.pvt  2  $xid2  9  10  11  10" # Instance 2, eth4
#      "0x7e00 ${hostname}-5.tenge.pvt  3  $xid3 12  13  14  13" # Instance 3, eth5
#    );;

    instances=(
      # Setup parameters for four instances.
      # 2 x E5-2630 (6-cores @ 2.3 GHz, 15 MB L3, 7.2 GT/s QPI, 1333 MHz DRAM)
      # Fluff thread and output thread share a core.
      # Save core  0 for OS.
      # Save core  5 for eth2 and eth3
      # Save core  6 for symmetry with core 0
      # Save core 11 for eth4 and eth5
      #
      # Setup for two GPU devices (two TITANs).
      #
      #                               GPU       NET FLF GPU OUT
      # mask  bind_host               DEV  XID  CPU CPU CPU CPU
      "0x001e ${hostname}-2.tenge.pvt  0  $xid0  1   2   3   3" # Instance 0, eth2
      "0x001e ${hostname}-3.tenge.pvt  0  $xid1  4   2   3   3" # Instance 1, eth3
      "0x0780 ${hostname}-4.tenge.pvt  1  $xid2  7   8   9   9" # Instance 2, eth4
      "0x0780 ${hostname}-5.tenge.pvt  1  $xid3 10   8   9   9" # Instance 3, eth5
    );;

  asa*)
    # Calculate XIDs based on mypx
    xid0=$(( 2*(mypx-1)    ))
    xid1=$(( 2*(mypx-1) + 1))

    instances=(
      # Setup parameters for two instances.
      # Fluff thread and output thread share a core.
      #
      #                               GPU       NET FLF GPU OUT
      # mask  bind_host               DEV  XID  CPU CPU CPU CPU
      "0x0707 ${hostname}-2.tenge.pvt  0  $xid0  2   8   1   8" # Instance 0, eth2
      "0x7070 ${hostname}-4.tenge.pvt  1  $xid1  6  12   5  12" # Instance 1, eth4
    );;

  simech1)
    # Calculate XIDs based on mypx
    xid0=$(( 2*(mypx-1)    ))
    xid1=$(( 2*(mypx-1) + 1))

    instances=(
      #                               GPU       NET FLF GPU OUT
      # mask  bind_host               DEV  XID  CPU CPU CPU CPU
      "0x0707 ${hostname}-2.tenge.pvt  0  $xid0  2   8   1   8" # Instance 0, eth2
      "0x7070 ${hostname}-3.tenge.pvt  1  $xid1  6  12   5  12" # Instance 1, eth3
    );;

  paper5)
    # Calculate XIDs based on mypx
    xid0=$(( 1*(mypx-1)    ))

    instances=( 
      #                               GPU       NET FLF GPU OUT
      # mask  bind_host               DEV  XID  CPU CPU CPU CPU
      "0x0606 ${hostname}-2.tenge.pvt  0  $xid0  2   4   3   4" # Instance 0, eth2
    );;

  px*)
    # Setup parameters for two instances.
    # 2 x E5-2620 v4 (disabled-HyperThreading,  8-cores @ 2.1 GHz, 20 MB L3, 8 GT/s QPI, 2667 MHz DRAM)
    # Fluff thread and output thread share a core.
    # Save core  0 for OS.
    # Save core  1 for eth2
    # Save core 8 for symmetry with core 0
    # Save core 9 for eth4 and eth5
    # ARP: on recommendations from JXK, DMM, changed NET CPU to avoid 0,8
    #
    # Setup for two GPU devices (two TITANs).
    #
    #                               GPU       NET FLF GPU OUT
    # mask  bind_host               DEV  XID  CPU CPU CPU CPU
    # Calculate XIDs based on mypx
    xid0=$(( 2*(mypx-1)    ))
    xid1=$(( 2*(mypx-1) + 1))

    instances=( 
      #                               GPU        IBV  NET    FLF   GPU  OUT  BDA
      # mask  bind_host               DEV  XID   CPU  CPU    CPU   CPU  CPU  CPU
      "0x00ff eth3                     0  $xid0   3    7   0x0006   4    5    6" # Instance 0, eth3
      "0xff00 eth5                     1  $xid1  11   15   0x0600  12   13   14" # Instance 1, eth5
    );;

  *)
    echo "This host (${hostname}) is not supported by $(basename $0)"
    exit 1
    ;;
esac

function init() {
  instance=$1
  mask=$2
  bindhost=$3
  gpudev=$4
  xid=$5
  ibvcpu=$6
  netcpu=$7
  flfcpu=$8
  gpucpu=$9
  outcpu=${10}
  bdacpu=${11}

  if [ -z "${mask}" ]
  then
    echo "Invalid instance number '${instance}' (ignored)"
    return 1
  fi

  if [ -z "$outcpu" ]
  then
    echo "Invalid configuration for host ${hostname} instance ${instance} (ignored)"
    return 1
  fi

  if [ $USE_IBVERBS -eq 1 ]
  then
    #netthread=hera_ibv_thread
    ibvthread="ibvpkt_thread"
    netthread="hera_ibvpkt_thread"
  else
    netthread=hera_pktsock_thread
  fi

  echo "Using netthread: $netthread"


  if [ $USE_TEST -eq 1 ]
  then
    echo "launching BDA in TEST VECTOR mode"
    echo taskset $mask \
    $hashpipe -p $plugin -I $instance \
      -c $gpucpu hera_fake_gpu_thread \
      -c $bdacpu hera_gpu_bda_thread \
      -c $outcpu hera_bda_output_thread
    taskset $mask \
    $hashpipe -p $plugin -I $instance \
      -o XID=$xid \
      $extraopts \
      -c $gpucpu hera_fake_gpu_thread \
      -c $bdacpu hera_gpu_bda_thread \
      -c $outcpu hera_bda_output_thread \
       < /dev/null \
      1> px${mypx}.out.$instance \
      2> px${mypx}.err.$instance &

  else
    echo "Using baseline dependent averaging"
    echo taskset $mask \
    $hashpipe -p $plugin -I $instance \
      -o BINDHOST=$bindhost \
      -o GPUDEV=$gpudev \
      -o XID=$xid \
      $extraopts \
      ${ibvthread:+-c $ibvcpu $ibvthread} \
      -c $netcpu $netthread \
      -m $flfcpu paper_fluff_thread \
      -c $gpucpu paper_gpu_thread \
      -c $bdacpu hera_gpu_bda_thread \
      -c $outcpu hera_bda_output_thread
    if [ $USE_REDIS -eq 1 ]
    then
      echo "Using redis logger"
      { taskset $mask \
      $hashpipe -p $plugin -I $instance \
        -o BINDHOST=$bindhost \
        -o GPUDEV=$gpudev \
        -o XID=$xid \
        $extraopts \
        ${ibvthread:+-c $ibvcpu $ibvthread} \
        -c $netcpu $netthread \
        -m $flfcpu paper_fluff_thread \
        -c $gpucpu paper_gpu_thread \
        -c $bdacpu hera_gpu_bda_thread \
        -c $outcpu hera_bda_output_thread \
      < /dev/null 2>&3 1>px${mypx}.out.$instance; } \
      3>&1 1>&2 | tee px${mypx}.err.$instance | \
      stdin_to_redis.py -l WARNING > /dev/null &
    else
      echo "*NOT* using redis logger"
      taskset $mask \
      $hashpipe -p $plugin -I $instance \
        -o BINDHOST=$bindhost \
        -o GPUDEV=$gpudev \
        -o XID=$xid \
        $extraopts \
        ${ibvthread:+-c $ibvcpu $ibvthread} \
        -c $netcpu $netthread \
        -m $flfcpu paper_fluff_thread \
        -c $gpucpu paper_gpu_thread \
        -c $bdacpu hera_gpu_bda_thread \
        -c $outcpu hera_bda_output_thread \
         < /dev/null \
        1> px${mypx}.out.$instance \
        2> px${mypx}.err.$instance &
    fi
  fi
}

# Default to Packet sockets; No redis logging
USE_IBVERBS=1
USE_REDIS=0
USE_TEST=0

for arg in $@; do
  case $arg in
    -h)
      echo "Usage: $(basename $0) [-r] [-i] INSTANCE_ID [...]"
      echo "  -r : Use redis logging (in addition to log files)"
      echo "  -i : Do not use IB-verbs pipeline (rather than packet sockets)"
      echo "  -t : Run BDA in test vector mode"
      exit 0
    ;;

    -i)
      USE_IBVERBS=0
      shift
    ;;
    -r)
      USE_REDIS=1
      shift
    ;;
    -t)
      USE_TEST=1
      shift
    ;;
  esac
done

if [ -z "$1" ]
then
  echo "Usage: $(basename $0) [-r] [-i] [-a] INSTANCE_ID [...]"
  echo "  -r : Use redis logging (in addition to log files)"
  echo "  -i : Do not use IB-verbs pipeline (rather than packet sockets)"
  echo "  -t : Lauch BDA in test vector mode"
  exit 1
fi

for instidx in "$@"
do
  args="${instances[$instidx]}"
  if [ -n "${args}" ]
  then
    echo
    echo Starting instance px$mypx/$instidx
    init $instidx $args
    echo Instance px$mypx/$instidx pid $!
    # Sleep to let instance come up
    sleep 10
  else
    echo Instance $instidx not defined for host $hostname
  fi
done

# Zero out MISSEDPK counts
for instidx in "$@"
do
  echo Resetting MISSEDPK counts for px$mypx/$instidx
  hashpipe_check_status -I $instidx -k MISSEDPK -s 0
done
