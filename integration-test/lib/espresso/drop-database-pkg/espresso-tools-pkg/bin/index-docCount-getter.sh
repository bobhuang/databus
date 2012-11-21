#!/bin/bash

script_dir=`dirname $0`
config_dir=$script_dir/../conf
source $script_dir/setup.inc
JVM_ARGS="-Xms256m -Xmx256m"
export DEFAULT_JAVA_HOME=/export/apps/jdk/JDK-1_6_0_21

${JAVA_HOME-$DEFAULT_JAVA_HOME}/bin/java  ${JVM_ARGS} -cp ${cp} com.linkedin.espresso.index.IndexDocCountGetter $@
