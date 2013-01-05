#!/bin/bash

ver=2.1.114-SNAPSHOT
script_dir=`dirname $0`
echo script_dir=${script_dir}
source ${script_dir}/common.inc

remote_scripts_dir=${script_dir}/../remote
build_dir=${script_dir}/../../../../
cmdtools_build_dir=${build_dir}/databus2-cmdline-tools-pkg/distributions
cmdtools_pkg=databus2-cmdline-tools-pkg-$ver.tar.gz
data_dir=$HOME/Documents/socc-data

function do_sync() {
  host=$1
  echo "Synching to $host"
  rsync -upEr ${remote_scripts_dir} $host:tools
  rsync -upE ${cmdtools_build_dir}/${cmdtools_pkg} $host:
  echo "Extracting cmdtools package on $host "
  $SSH -tt $host "mkdir -p tools && rm -rf tools/lib/* && tar -zxf ${cmdtools_pkg} -C tools"
  echo "Synching data with $host"
  mkdir -p ${data_dir}/$host
  rsync -upEr $host:result*.out $host:top*.out $host:stack*.txt ${data_dir}/$host/
  echo "Synched $host"
}


for h in $remote_hosts_all ; do
  do_sync $h &
done
