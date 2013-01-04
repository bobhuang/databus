#!/bin/bash

script_dir=`dirname $0`
source $script_dir/setup.inc
main_class=com.linkedin.databus2.bootstrap.producer.perf.ProducerSimulatorMain

${script_dir}/database/databus2-bootstrap/bootstrap_db_reinit.sh 2>&1 > $log_dir/db_reinit.log

java -cp ${cp} ${main_class} $*