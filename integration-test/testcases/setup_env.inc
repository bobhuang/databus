# set up env, figure out the path

if [ -z "${TEST_NAME}" ] ; then
    exit 1 
fi

#basic paths
export TEST_NAME
export LOG_SUB_DIR=log
export WORK_SUB_DIR=log
export VAR_DIR=../var
export SCRIPT_DIR=../script
export CONFIG_DIR=integration-test/config
export WORK_DIR=../var/${WORK_SUB_DIR}/${TEST_NAME}
export LOG_DIR=../var/${LOG_SUB_DIR}/${TEST_NAME}
export WORK_DIR_FROM_ROOT=integration-test/var/${WORK_SUB_DIR}/${TEST_NAME}
export LOG_DIR_FROM_ROOT=integration-test/var/${LOG_SUB_DIR}/${TEST_NAME}
export CONFIG_DIR_FROM_ROOT=integration-test/config
export DATA_DIR=../data
export VIEW_ROOT=`cd ../../; echo $PWD`

#port mapping
#relay
export RELAY_PORT_BASE=9000
#bootstrap
export BOOTSTRAP_SERVER_PORT_BASE=6060
export BOOTSTRAP_PRODUCER_PORT_BASE=6000
#clients
export CLIENT_PORT_BASE=10564

#war ports
BOOTSTRAP_SERVER_PORT=11111
BOOTSTRAP_SERVER_CONTAINER_PORT=11110
ANET_RELAY_PORT=11269
ANET_RELAY_CONTAINER_PORT=11268
ANET_BOOTSTRAP_PRODUCER_PORT=11271
ANET_BOOTSTRAP_PRODUCER_CONTAINER_PORT=11270
BIZFOLLOW_RELAY_PORT=9190
BIZFOLLOW_RELAY_CONTAINER_PORT=11106
BIZFOLLOW_BOOTSTRAP_PRODUCER_PORT=11109
BIZFOLLOW_BOOTSTRAP_PRODUCER_CONTAINER_PORT=11108
CONN_RELAY_PORT=11281
CONN_RELAY_CONTAINER_PORT=11280
LIAR_RELAY_PORT=9091
LIAR_RELAY_CONTAINER_PORT=11114
LIAR_BOOTSTRAP_PRODUCER_PORT=11117
LIAR_BOOTSTRAP_PRODUCER_CONTAINER_PORT=11116
MEMBER2_RELAY_PORT=11183
MEMBER2_RELAY_CONTAINER_PORT=11182
MEMBER2_BOOTSTRAP_PRODUCER_PORT=11185
MEMBER2_BOOTSTRAP_PRODUCER_CONTAINER_PORT=11184
TEST_CLIENT_PORT=11113
TEST_CLIENT_CONTAINER_PORT=11112
TEST_MULTICLIENT_PORT=11578
TEST_MULTICLIENT_CONTAINER_PORT=11577
CONN_BOOTSTRAP_PRODUCER_PORT=11273
CONN_BOOTSTRAP_PRODUCER_CONTAINER_PORT=11272

export ZK_HOST=`hostname`
export ZK_PORT=2181
export HELIX_HOST=`hostname`
export HELIX_PORT_ADMIN=12925
export HELIX_PORT=12926

# solaris tail
export TAIL_PATH=/usr/xpg4/bin/tail     
if [ ! -f $TAIL_PATH ]; then
  export TAIL_PATH=tail
fi

# over all stats
all_stat=0

# helper functions
function assert_with_retry {
  retries=$1
  shift
  assert=$*
  
  success=0
  for ((; retries > 0; --retries)) ; do
     if eval $assert ; then
       success=1
       break
     fi 
     sleep 1
  done
  test "$success" == "1"
}

function assert_false_with_retry {
  retries=$1
  shift
  assert=$*
  
  success=0
  for ((; retries > 0; --retries)) ; do
     if eval $assert ; then
       success=0
     else
       success=1
       break
     fi 
     sleep 1
  done
  test "$success" == "1"
}

