#!/bin/bash

script_dir=`dirname $0`
source ${script_dir}/setup.inc

java -Xms256m -Xmx256m -cp $cp com.linkedin.databus2.hadoop.InflateMain $*