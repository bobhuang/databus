#!/bin/bash

script_dir=`dirname $0`
source $script_dir/setup.inc

main_class=com.linkedin.databus2.tool.DbusEventToolMain

java -cp $cp ${main_class} $*
