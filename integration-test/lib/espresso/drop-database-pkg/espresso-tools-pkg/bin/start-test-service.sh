#!/bin/bash

if [ -z "${JAVA_HOME}" ]; then
   JAVA_HOME=/export/apps/jdk/JDK-1_6_0_21
fi

# Runs the Data Generator class
script_dir=`dirname $0`
config_dir=$script_dir/../config
source $script_dir/setup.inc
JVM_ARGS="-Xms256m -Xmx256m"

${JAVA_HOME}/bin/java ${JVM_ARGS} -Dlog4j.configuration=$config_dir/log4j.xml -cp ${cp} com.linkedin.espresso.index.LightWeightRequestTool $@

