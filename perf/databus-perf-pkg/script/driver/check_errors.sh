#!/bin/bash

script_dir=`dirname $0`
echo script_dir=${script_dir}
source ${script_dir}/common.inc

function do_check() {
  host=$1
  echo "************ Checking $h ..."
  $SSH -tt $host "egrep 'Exception|ERROR|WARN' nohup.out" 
}


for h in ${remote_hosts_all} ; do
  do_check $h
done
