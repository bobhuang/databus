

function download_etools()
{
  mkdir -p ${ESPRESSO_TOOLS_PATH}
  pull-artifact com.linkedin.espresso espresso-tools-pkg ${ESPRESSO_VERSION} tar.gz .
  etools_targz_name=espresso-tools-pkg-${ESPRESSO_VERSION}.tar.gz
  if [ ! -f ${etools_targz_name} ] ; then
    echo "$0: download of espresso tools package ${etools_targz_name} failed!"
    exit 1
  fi
  tar -zxf ${etools_targz_name} -C ${ESPRESSO_TOOLS_PATH}
}

function discover_ip() {
	export server_ip=`ifconfig -a | grep 'inet addr' | grep -v '127\.0\.0\.1' | sed -e 's/.*inet addr:\([^ ]*\).*/\1/'`
	if [ -z "${server_ip}" ] ; then
  		echo "$0: failed to determine server id"
  		exit 1
	fi

	export default_server_id=`perl -e 'if ($ARGV[0] =~ /[\d\.]+\.(\d+)\.(\d+)/) {print $1*256 + $2;}' ${server_ip}`
	if [ -z "${default_server_id}" ]; then
  		echo "$0: unable to calculate base server_id"
  		exit 1
	fi 
}

function mysql_instance_id() {
	port=$1
	echo $((default_server_id +  port * 65536))
}

