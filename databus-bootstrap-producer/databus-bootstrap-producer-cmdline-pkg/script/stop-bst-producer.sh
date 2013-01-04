#!/bin/bash

cd `dirname $0`/..


source_name=$1
if [ -z "${source_name}" ] ; then
  echo "USAGE: $0 source_name [args]"
  exit 1 
fi
shift

script_dir=./bin
source $script_dir/setup.inc
source $script_dir/setup-bootstrap.inc
source $script_dir/setup-producer.inc


if [ -f ${bstprod_pid_file} ] ; then
  kill `cat ${bstprod_pid_file}`
else
  echo "$0: unable to find PID file ${bstprod_pid_file}"
fi