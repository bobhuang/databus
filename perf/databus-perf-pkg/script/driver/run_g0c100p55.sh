#!/bin/bash

script_dir=`dirname $0`
exp=g0c100p55
source ${script_dir}/common.inc

for h in ${remote_hosts_all} ; do
  echo "Starting client $h"
  $SSH -tt $h "rm -f nohup.out && nohup tools/remote/run_n_consumers.sh $exp $relay:11183 com.linkedin.events.member2.profile.MemberProfile 2000 50000000 55 ${run_time}s $ts 10"
done

echo
echo "Waiting for experiments to finish ..."
sleep $((run_time+60))

${script_dir}/sync_remote.sh
get_relay_logs $exp

#ssh -tt $relay 'nohup relay01/bin/stop-perf-relay.sh member2_mmap'
