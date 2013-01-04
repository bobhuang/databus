#!/bin/bash

# validate mysql replication setup
script_dir=`dirname $0`
config_dir=$script_dir/../conf
config_dir=$(cd "${config_dir}" && pwd)
source $script_dir/setup.inc

${JAVA_HOME}/bin/java ${JVM_ARGS} -Dlog4j.configuration=file://$config_dir/log4j.xml -cp ${cp} $@

