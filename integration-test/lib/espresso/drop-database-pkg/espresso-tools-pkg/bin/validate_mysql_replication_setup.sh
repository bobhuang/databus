#!/bin/bash

# validate mysql replication setup
script_dir=`dirname $0`
config_dir=$script_dir/../config
source $script_dir/setup.inc

#${JAVA_HOME}/bin/java ${JVM_ARGS} -Dlog4j.configuration=file://$config_dir/log4j-info.xml -cp ${cp} com.linkedin.espresso.storagenode.cluster.MysqlReplicationSetupValidator $@
${JAVA_HOME}/bin/java ${JVM_ARGS} -Dlog4j.configuration=file://$config_dir/log4j-info.properties -cp ${cp} com.linkedin.espresso.storagenode.replication.MysqlReplicationSetupValidator $@

