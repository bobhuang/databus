#!/bin/bash

script_dir=`dirname $0`
source ${script_dir}/common.inc
test_name=g0c20p100

ts=`date +%y%m%d%H%M%S`

for h in ${remote_hosts} ; do
  echo "Starting client $h"
  $SSH -tt $h "rm -f nohup.out && nohup tools/remote/run_n_consumers.sh $test_name $relay:11183 com.linkedin.events.member2.profile.MemberProfile 2000 50000000 100 ${run_time}s $ts 4"
done

echo
echo "Waiting for experiments to finish ..."
sleep $((run_time+60))

${script_dir}/sync_remote.sh
