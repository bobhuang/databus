#!/bin/bash

script_dir=`dirname $0`
source ${script_dir}/common.inc
exp=g1000c80p10

stop_relay member2_mmap
sleep 5
clean_relay_logs
start_relay member2_mmap

for h in ${remote_hosts_all} ; do
  ssh -tt $h "rm -f nohup.out && nohup tools/remote/run_n_consumers.sh $exp $relay:11183 com.linkedin.events.member2.profile.MemberProfile 2000 50000000 10 ${run_time}s $ts 8"
done

sleep 15
${genscript} -s 40 -b 1000 -e 1000 -t 1800000 --server_host=$relay --server_port=11183
sleep $((run_time+60))

${script_dir}/sync_remote.sh
get_relay_logs $exp

