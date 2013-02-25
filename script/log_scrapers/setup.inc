date_path=`date +%Y/%m/%d/%H`
report_date_str=`date +'%Y-%m-%d %H:00'`
data_log_dir="logs/${date_path}"
report_dir="reports/${date_path}"

function smart_ssh() {
	while [ "${1:0:1}" == "-" ] ; do
		opts="$opts $1"
		shift
	done
	if ( echo $1 | grep '\.prod' ) ; then
		ssh_cmd='ssh -ttA eng-portal.corp.linkedin.com ssh'
	else
		ssh_cmd='ssh -tt'
	fi
	${ssh_cmd} ${opts} $*
}

