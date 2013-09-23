#!/bin/bash

PATH="$(dirname $0):${PATH}"

# Set default instance id
instance_id=0

# Set default intcount
intcount=4096

# Set intsync to empty value
intsync=

function get_sync_mcnt() {
  echo $(( $(hashpipe_check_status -I $instance_id -Q GPUMCNT 2>/dev/null) + 8192 ))
}

function start() {
  if [ -z "${intsync}" ]
  then
    intsync=$(get_sync_mcnt)
    if [ -z "${intsync}" ]
    then
      echo "Unable to get GPUMCNT from instance ${instance_id}"
      exit 1
    fi
  fi

  hashpipe_check_status -I $instance_id -k INTSYNC  -s $intsync
  hashpipe_check_status -I $instance_id -k INTCOUNT -s $intcount
  hashpipe_check_status -I $instance_id -k INTSTAT  -s start
}

function stop() {
  hashpipe_check_status -I $instance_id -k INTSTAT  -s stop
}

function help() {
  echo "$(basename $0) [-n INTCOUNT] [INSTANCE_ID] CMD [[INSTANCE_ID] CMD ...]"
  echo 'CMD can be "start" or "stop"'
}

if [ "${1}" == "-n" ]
then
  shift
  intcount=$(($1))
  shift
fi

if [ -z "$1" ]
then
  help
  exit 1
fi

for arg in "${@}"
do
  case $arg in
    [0-9]*) instance_id=$arg;;
    start) start;;
    stop) stop;;
    test) get_sync_mcnt;;
    *) help; exit 1;;
  esac
done
