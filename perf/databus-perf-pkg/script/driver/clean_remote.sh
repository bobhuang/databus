#!/bin/bash

pattern=$*

script_dir=`dirname $0`
echo script_dir=${script_dir}
source ${script_dir}/common.inc

function do_remove() {
  host=$1
  echo "Cleaning $h ..."
  $SSH -tt $host "rm -f $pattern" 
  echo "Cleaned $h !"
}


for h in ${remote_hosts_all} ; do
  do_remove $h &
done
