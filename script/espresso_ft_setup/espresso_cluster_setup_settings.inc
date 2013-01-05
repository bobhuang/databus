ESPRESSO_CLUSTER_NAME=ESPRESSO_MULTITENANT_1_EI
RELAY_CLUSTER_NAME=RELAY_MULTITENANT_1_EI
SUPER_CLUSTER_NAME=DevClustersV5
declare -a RELAY_HOSTS=( "eat1-app141.corp.linkedin.com" "eat1-app64.corp.linkedin.com" "eat1-app65.corp.linkedin.com" "eat1-app66.corp.linkedin.com" )
declare -a RELAY_INSTANCES=( "01" "02" "03" "04" "05" )
declare -a SN_HOSTS=( "eat1-app15.corp.linkedin.com" "eat1-app17.corp.linkedin.com" "eat1-app18.corp.linkedin.com" "eat1-app19.corp.linkedin.com" "eat1-app22.corp.linkedin.com" "eat1-app84.corp.linkedin.com" )
DEFAULT_MYSQL_MASTER_PORT=3306
CLUSTER_FABRIC=EI
RELAY_REPL_FACTOR=3
RELAY_BUFFER_HOURSPAN=48

HELIX_ADMIN_URL=eat1-app114.corp.linkedin.com:12924
ESPRESSO_TOOLS_PATH=${script_dir}/etools
ZK_URL=eat1-app207.stg.linkedin.com:12913
ESPRESSO_VERSION=0.5.146
DBUS_CONFIG_PATH=${script_dir}/databus2-trunk-cfg
RPL_DBUS_ROOT_PWD=
RPL_DBUS_USER_NAME=rplespresso
RPL_DBUS_USER_PWD=espresso
SN_MYSQL_USER_NAME=espresso
SN_MYSQL_USER_PWD=espresso

#SETTINGS RARELY TO BE CHANGED
RELAY_HTTP_PORT_PREFIX=281
RELAY_TCP_PORT_PREFIX=280
RPLDBUS_PORT_PREFIX=290
DEFAULT_MYSQL_MASTER_HOST="${SN_HOSTS[0]}"
