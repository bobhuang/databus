#!/bin/bash

script_dir=`dirname $0`
source ${script_dir}/espresso_cluster_setup_settings.inc
source ${script_dir}/common_funcs.inc

function usage() {
	echo "USAGE: $0 instance cmd [options]"
        echo "       instance: 01, 02, 03, ..."
        echo "        cmd: start stop mysql params"
	exit 0
}

function do_mysql() {
	declare -a opt=( "$*" )
	if [ ! -z "${RPL_DBUS_USER_PWD}" ] ; then
		pwd_opt="-p${RPL_DBUS_USER_PWD}"
	else
		pwd_opt=
	fi
	mysql --defaults-file=${instance_dir}/my.cnf -u${RPL_DBUS_USER_NAME} $pwd_opt "${opt[@]}"
}

function do_params() {
	echo "port: ${instance_port}"
	echo "id: ${instance_id}"
	echo "dir: ${instance_dir}"
}

function do_start() {
	sudo -uapp mysqld_safe --defaults-file=${instance_dir}/my.cnf &
}

function do_stop() {
	if [ ! -z "${RPL_DBUS_ROOT_PWD}" ] ; then
		pwd_opt="-p${RPL_DBUS_ROOT_PWD}"
	else
		pwd_opt=
	fi
	sudo -uapp mysqladmin --defaults-file=${instance_dir}/my.cnf -uroot $pwd_opt shutdown
}

###### main()
instance=$1
cmd=$2
shift 2
declare -a opts=( "$*" )

if [ -z "$instance" -o -z "$cmd" ] ; then
	usage
fi

discover_ip

export instance_port=${RPLDBUS_PORT_PREFIX}${instance}
export instance_id=`mysql_instance_id ${instance_port}`	
export instance_dir="/export/apps/mysql-${instance_id}"

case $cmd in

mysql) do_mysql "${opts[@]}" ;;
params) do_params ;;
start) do_start ;;
stop) do_stop ;;
*) echo "unknown command: $cmd" ;;

esac 
