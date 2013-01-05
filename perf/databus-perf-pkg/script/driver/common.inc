int_scripts=${script_dir}/../../../../../integration-test/script
genscript=${int_scripts}/dbus2_gen_event.py
data_dir=${script_dir}/../../data
remote_scripts_dir=${script_dir}/../remote
build_dir=${script_dir}/../../../../
cmdtools_build_dir=${build_dir}/databus2-cmdline-tools-pkg/distributions
cmdtools_pkg=databus2-cmdline-tools-pkg-$ver.tar.gz
data_dir=${HOME}/Documents/socc-data

mkdir -p ${data_dir}

ver=2.1.114-SNAPSHOT

hostn=10
remote_hosts="eat1-app164.corp.linkedin.com eat1-app170.corp.linkedin.com eat1-app104.corp.linkedin.com eat1-app160.corp.linkedin.com eat1-app158.corp.linkedin.com"
remote_hosts_all="eat1-app164.corp.linkedin.com eat1-app170.corp.linkedin.com eat1-app104.corp.linkedin.com eat1-app160.corp.linkedin.com eat1-app158.corp.linkedin.com eat1-app159.corp.linkedin.com eat1-app161.corp.linkedin.com eat1-app182.corp.linkedin.com eat1-app183.corp.linkedin.com eat1-app184.corp.linkedin.com"
relay=eat1-app168.corp.linkedin.com

ts=`date +%y%m%d%H%M%S`
run_time=300

export SSH="ssh -qo StrictHostKeyChecking=no"
export SCP="scp -o StrictHostKeyChecking=no"
export RSYNC_RSH="$SSH"

function clean_relay_logs() {
	$SSH -tt $relay 'rm -f nohup.out relays/logs/*.log relays/logs/*.out'
}

function start_relay() {
	source=$1
	$SSH -tt $relay "nohup relays/bin/start-perf-relay.sh $source"
}

function stop_relay() {
	source=$1
	$SSH -tt $relay "nohup relays/bin/stop-perf-relay.sh $source"
}

function get_relay_logs() {
	exp=$1
	$SCP $relay:relays/logs/relay.log ${data_dir}/relay-$exp.$ts.log
	$SCP $relay:relays/logs/gc.log ${data_dir}/gc-$exp.$ts.log
	gzip ${data_dir}/gc-$exp.$ts.log &
	$SCP $relay:relays/logs/http.log ${data_dir}/http-$exp.$ts.log
	gzip ${data_dir}/http-$exp.$ts.log &
}
