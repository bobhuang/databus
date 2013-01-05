#!/bin/bash

script_dir=`dirname $0`
source ${script_dir}/common.inc
test_name=g0c80p10

ts=`date +%y%m%d%H%M%S`

for h in ${remote_hosts_all} ; do
  echo "Starting client $h"
  $SSH -tt $h "rm -f nohup.out && nohup tools/remote/run_n_consumers.sh $test_name $relay:11183 com.linkedin.events.member2.profile.MemberProfile 2000 30000000 10 ${run_time}s $ts 8"
done

echo
echo "Waiting for experiments to finish ..."
sleep $((run_time+60))

${script_dir}/sync_remote.sh
