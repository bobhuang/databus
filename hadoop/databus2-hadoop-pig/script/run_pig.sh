#!/bin/bash

script_dir=`dirname $0`
source ${script_dir}/setup.inc

source ${script_dir}/hadoop-setup.inc

PIG_JAVA_OPTS="-Dmapred.job.queue.name=marathon" pig $*