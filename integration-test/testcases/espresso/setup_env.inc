# set up env, figure out the path

if [ -z "${TEST_NAME}" ] ; then
    exit 1 
fi

#basic paths
export TEST_NAME
export LOG_SUB_DIR=log
export WORK_SUB_DIR=log
export VAR_DIR=../../var
export SCRIPT_DIR=../../script
export CONFIG_DIR=integration-test/config
export WORK_DIR=../../var/${WORK_SUB_DIR}/${TEST_NAME}
export LOG_DIR=../../var/${LOG_SUB_DIR}/${TEST_NAME}
export WORK_DIR_FROM_ROOT=integration-test/var/${WORK_SUB_DIR}/${TEST_NAME}
export LOG_DIR_FROM_ROOT=integration-test/var/${LOG_SUB_DIR}/${TEST_NAME}
export DATA_DIR=../data
export VIEW_ROOT=`cd ../../../; echo $PWD`
export ZK_HOST=`hostname`
export ZK_PORT=2181
export HELIX_HOST=`hostname`
export HELIX_PORT_ADMIN=12925
export HELIX_PORT=
export ESPRESSO_ROUTER_HOST=`hostname`
export ESPRESSO_ROUTER_PORT=12921

#cluster manager related 
export superClusterName=DevClustersV4
export relay1Name=localhost_11140
export relay1Port=11140
export relay1TCPPort=11141
export relay2Name=localhost_11150
export relay2Port=11150
export relay2TCPPort=11151
export relay3Name=localhost_11160
export relay3Port=11160
export relay3TCPPort=11161
export relay4Name=localhost_11170
export relay4Port=11170
export relay4TCPPort=11171
export relay5Name=localhost_11180
export relay5Port=11180
export relay5TCPPort=11181
export StorageNodeList=eat1-app32.corp.linkedin.com,eat1-app122.corp.linkedin.com,localhost
export SlaveThreadNames=devClusterNode1,devClusterNode2,default
export EspressoRouter=eat1-app32.corp.linkedin.com
export EspressoRouterPort=12921
export zkServer=eat1-app207.stg:12913

#port mapping
#relay
export RELAY_PORT_BASE=8887
#bootstrap
export BOOTSTRAP_SERVER_PORT_BASE=6060
export BOOTSTRAP_PRODUCER_PORT_BASE=6000
#clients
export CLIENT_PORT_BASE=10564

# solaris tail
export TAIL_PATH=/usr/xpg4/bin/tail     
if [ ! -f $TAIL_PATH ]; then
  export TAIL_PATH=tail
fi


# over all stats
all_stat=0
