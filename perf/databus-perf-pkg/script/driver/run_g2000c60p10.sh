#!/bin/bash

script_dir=`dirname $0`
exp=g2000c60p10
source ${script_dir}/common.inc

clean_relay_logs
stop_relay member2_mmap
start_relay member2_mmap

for h in ${remote_hosts_all} ; do
  ssh -tt $h "rm -f nohup.out && nohup tools/remote/run_n_consumers.sh $exp $relay:11183 com.linkedin.events.member2.profile.MemberProfile 2000 50000000 10 720s $ts 6"
done

sleep 15
${genscript} -s 40 -b 2000 -e 2000 -t 1800000 --server_host=$relay --server_port=11183
sleep 780

${script_dir}/sync_remote.sh
get_relay_logs $exp


#ssh -tt $relay 'nohup relay01/bin/stop-perf-relay.sh member2_mmap'
