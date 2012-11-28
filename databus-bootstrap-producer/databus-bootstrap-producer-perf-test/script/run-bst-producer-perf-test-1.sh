#!/bin/bash

script_dir=`dirname $0`
source ${script_dir}/setup.inc

${script_dir}/run-bst-producer-perf-test.sh -c ${conf_dir}/test_bootstrap_producer.conf -e ${conf_dir}/test_event_producer.conf $*