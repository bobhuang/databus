#!/bin/bash
#
#
# Copyright 2013 LinkedIn Corp. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#

cd `dirname $0`/..

script_dir=./bin
source $script_dir/setup.inc

if [ -z "$JAVA_HOME" ] ; then
	export JAVA_HOME=/export/apps/jdk/JDK-1_6_0_21/
	export PATH=$JAVA_HOME/bin:$PATH
fi

# DEFAULT VALUES
relay_type=default
jvm_gc_log=${logs_dir}/gc.log
db_relay_config=

# JVM ARGUMENTS
jvm_direct_memory_size=2048m
jvm_direct_memory="-XX:MaxDirectMemorySize=${jvm_direct_memory_size}"
jvm_min_heap_size="512m"
jvm_min_heap="-Xms${jvm_min_heap_size}"
jvm_max_heap_size="512m"
jvm_max_heap="-Xmx${jvm_max_heap_size}"

jvm_gc_log_option=
if [ ! -z "${jvm_gc_log}" ] ; then
  jvm_gc_log_option="-Xloggc:${jvm_gc_log}"
fi

jvm_arg_line="-d64 ${jvm_direct_memory} ${jvm_min_heap} ${jvm_max_heap} ${jvm_gc_log_option} -ea"

phys_conf_files=`ls ${conf_dir}/*.json`
phys_conf_files=`echo $phys_conf_files | sed -e 's/ /,/g'`

log4j_file_option="-l ${conf_dir}/espresso_relay_log4j.properties"
config_file_option="-p ${conf_dir}/espresso_relay_stg.properties"
#java_arg_line="${config_file_option} ${log4j_file_option} --db_relay_config ${phys_conf_files}"
java_arg_line="${config_file_option} ${log4j_file_option}"

main_class=com.linkedin.databus3.espresso.EspressoRelay

cmdline="java -cp ${cp} ${jvm_arg_line} ${main_class} ${java_arg_line} $*"
echo $cmdline
nohup $cmdline  2>&1 > ${espresso_out_file} &
echo $! > ${espresso_pid_file}
