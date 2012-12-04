#!/bin/bash

#[ -z $LOCAL_JAVA_HOME ] && LOCAL_JAVA_HOME="/System/Library/Frameworks/JavaVM.framework/Versions/1.6/Home"
script_dir=`dirname $0`
config_dir=$script_dir/../conf
source $script_dir/setup.inc

${JAVA_HOME}/bin/java ${JVM_ARGS} -cp ${cp} \
  -Dlog4j.configuration="file://${config_dir}/log4j.xml" \
  com.linkedin.espresso.smoketest.SmokeTestRunner $*
#  -agentlib:jdwp=transport=dt_socket,suspend=y,address=localhost:8991,server=y \
