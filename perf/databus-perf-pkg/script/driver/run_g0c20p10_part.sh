#!/bin/bash

script_dir=`dirname $0`
exp=g0c20p10_part
source ${script_dir}/common.inc

groupn=0
for h in ${remote_hosts} ; do
  ssh -tt $h "nohup tools/remote/run_n_consumers.sh $exp $relay:11183 com.linkedin.events.member2.profile.MemberProfile 2000 50000000 10 600s $ts 4 20 $groupn"
  groupn=$((groupn+1))
done

sleep 660

${script_dir}/sync_remote.sh
get_relay_logs $exp


#ssh -tt $relay 'nohup relay01/bin/stop-perf-relay.sh member2_mmap'
