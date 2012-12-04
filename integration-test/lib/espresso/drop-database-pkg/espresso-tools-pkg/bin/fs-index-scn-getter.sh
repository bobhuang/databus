#!/bin/bash

script_dir=`dirname $0`
config_dir=$script_dir/../conf
source $script_dir/setup.inc
JVM_ARGS=-Xmx8g

${JAVA_HOME}/bin/java ${JVM_ARGS} -cp ${cp} com.linkedin.espresso.index.FileSystemIndexSCNGetter $@
