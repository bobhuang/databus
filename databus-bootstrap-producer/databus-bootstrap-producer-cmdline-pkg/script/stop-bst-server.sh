#!/bin/bash

cd `dirname $0`/..


script_dir=./bin
source $script_dir/setup.inc
source $script_dir/setup-bootstrap.inc
source $script_dir/setup-server.inc


if [ -f ${bstserver_pid_file} ] ; then
  kill `cat ${bstserver_pid_file}`
else
  echo "$0: unable to find PID file ${bstserver_pid_file}"
fi