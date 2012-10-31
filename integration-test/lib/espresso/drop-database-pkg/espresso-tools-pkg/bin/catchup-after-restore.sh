#!/bin/bash

# catchup mysql-replicated slave from master
script_dir=`dirname $0`
config_dir=../conf
log4jfile=log4j-info.properties

source $script_dir/setup.inc
cp ${config_dir}/${log4jfile} ${log4jfile}

echo ${JAVA_HOME}/bin/java ${JVM_ARGS} -Dlog4j.configuration=${log4jfile} -cp ${cp} com.linkedin.espresso.tools.cluster.CatchupAfterBootstrap $@
${JAVA_HOME}/bin/java ${JVM_ARGS} -Dlog4j.configuration=${log4jfile} -cp ${cp} com.linkedin.espresso.tools.cluster.CatchupAfterBootstrap $@

