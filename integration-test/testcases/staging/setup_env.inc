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
export DATA_DIR=../../data
export VIEW_ROOT=`cd ../../../; echo $PWD`

#port mapping
#relay
export RELAY_PORT_BASE=9000
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
