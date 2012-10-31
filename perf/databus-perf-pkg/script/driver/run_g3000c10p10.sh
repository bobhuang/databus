#!/bin/bash

script_dir=`dirname $0`
int_scripts=${script_dir}/../../../../integration-test/script
genscript=${int_scripts}/dbus2_gen_event.py

remote_hosts="eat1-app171.corp.linkedin.com eat1-app172.corp.linkedin.com eat1-app173.corp.linkedin.com eat1-app174.corp.linkedin.com eat1-app175.corp.linkedin.com"
ts=`date +%y%m%d%H%M%S`

ssh -tt eat1-app170.corp.linkedin.com 'nohup relay01/bin/stop-perf-relay.sh member2_mmap'
ssh -tt eat1-app170.corp.linkedin.com 'nohup relay01/bin/start-perf-relay.sh member2_mmap'


for h in ${remote_hosts} ; do
  ssh -tt $h "nohup tools/remote/run_n_consumers.sh g3000c10p10 eat1-app170.corp.linkedin.com:11183 com.linkedin.events.member2.profile.MemberProfile 2000 50000000 10 300s $ts 2"
done

${genscript} -s 40 -b 3000 -e 3000 -t 600000 --server_host=eat1-app170.corp.linkedin.com --server_port=11183

#ssh -tt eat1-app170.corp.linkedin.com 'nohup relay01/bin/stop-perf-relay.sh member2_mmap'
