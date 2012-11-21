#!/bin/bash

# Runs the Data Generator class
script_dir=`dirname $0`
config_dir=$script_dir/../config
source $script_dir/setup.inc

${JAVA_HOME}/bin/java ${JVM_ARGS} -Dlog4j.configuration=$config_dir/log4j.xml -cp ${cp} com.linkedin.espresso.schema.impl.ChecksumDocumentSchemaImpl $@

