#!/bin/bash

script_dir=`dirname $0`/dist/bin
source $script_dir/setup.inc

${JAVA_HOME}/bin/java ${JVM_ARGS} -cp ${cp} com.linkedin.espresso.storagenode.StorageNode $@
