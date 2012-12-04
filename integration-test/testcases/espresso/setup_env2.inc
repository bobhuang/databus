# set up env, figure out the path

if [ -z "${TEST_NAME}" ] ; then
    exit 1 
fi

#basic paths
export TEST_NAME
export LOG_SUB_DIR=log
export WORK_SUB_DIR=log
export VAR_DIR=../../var
export SCRIPT_DIR=`cd ../../script && pwd`
export CONFIG_DIR=integration-test/config
export WORK_DIR=../../var/${WORK_SUB_DIR}/${TEST_NAME}
export LOG_DIR=../../var/${LOG_SUB_DIR}/${TEST_NAME}
export WORK_DIR_FROM_ROOT=integration-test/var/${WORK_SUB_DIR}/${TEST_NAME}
export LOG_DIR_FROM_ROOT=integration-test/var/${LOG_SUB_DIR}/${TEST_NAME}
export DATA_DIR=../data
export VIEW_ROOT=`cd ../../../; echo $PWD`
export ZK_HOST=localhost
export ZK_PORT=2181
export RELAY_ZK_HOST=localhost
export RELAY_ZK_PORT=2181
export ZK_SCHEMA_ROOT_DIR=schemas_registry/espresso_test
export ZK_SCHEMA_ROOT_DIR_SRC=${VIEW_ROOT}/schemas_registry/espresso_test

#cluster manager related 
export STORAGE_NODE_CLUSTER=DevCluster_Dbus
export superClusterName=RelaySuperCluster
export relay1Host=localhost
export relay1Port=11140
export relay1TCPPort=11141
export relay1Name=${relay1Host}_${relay1Port}
export relay2Host=localhost
export relay2Port=11150
export relay2TCPPort=11151
export relay2Name=${relay2Host}_${relay2Port}
export relay3Host=localhost
export relay3Port=11160
export relay3TCPPort=11161
export relay3Name=${relay3Host}_${relay3Port}
export relay4Host=localhost
export relay4Port=11170
export relay4TCPPort=11171
export relay4Name=${relay4Host}_${relay4Port}
export relay5Host=localhost
export relay5Port=11180
export relay5TCPPort=11181
export relay5Name=${relay5Host}_${relay5Port}
export StorageNodeList=eat1-app32.corp.linkedin.com,eat1-app122.corp.linkedin.com,localhost
export SlaveThreadNames=devClusterNode1,devClusterNode2,default
export EspressoRouter=localhost
export EspressoRouterPort=12921
export zkServer=${ZK_HOST}:${ZK_PORT}
export zkServerRelay=${RELAY_ZK_HOST}:${RELAY_ZK_PORT}

#port mapping
#relay
export RELAY_PORT_BASE=8887
#bootstrap
export BOOTSTRAP_SERVER_PORT_BASE=6060
export BOOTSTRAP_PRODUCER_PORT_BASE=6000
#clients
export CLIENT_HOST=localhost
export CLIENT_PORT_BASE=10564
#storage node
export SN1_PORT=12918
export SN2_PORT=12818

# solaris tail
export TAIL_PATH=/usr/xpg4/bin/tail     
if [ ! -f $TAIL_PATH ]; then
  export TAIL_PATH=tail
fi

# over all stats
all_stat=0
