#!/bin/bash

script_dir=`dirname $0`
source $script_dir/setup.inc

# DEFAULT VALUES
relay_type=default
jvm_gc_log=${logs_dir}/gc.log
db_relay_config=

# COMMAND-LINE PROCESSING
process_args=1

while [ ! -z "$process_args" ] ; do

  case $1 in
  
     -type) relay_type=$2; shift 2;;
     -gc) jvm_gc_log=$2; shift 2;;
     -db_relay_config) db_relay_config=$2; shift 2;;
     *) process_args= ;;
     
  esac

done 


case $relay_type in

  default) main_class=com.linkedin.databus.container.netty.HttpRelay  ;;
  db_relay) main_class=com.linkedin.databus2.relay.DatabusRelayMain ;;  
  *) echo "$0: unknown relay type: $relay_type"; exit 1;;

esac

# JVM ARGUMENTS
jvm_direct_memory_size=2048m
jvm_direct_memory="-XX:MaxDirectMemorySize=${jvm_direct_memory_size}"
jvm_min_heap_size="100m"
jvm_min_heap="-Xms${jvm_min_heap_size}"
jvm_max_heap_size="512m"
jvm_max_heap="-Xmx${jvm_max_heap_size}"

jvm_gc_log_option=
if [ ! -z "${jvm_gc_log}" ] ; then
  jvm_gc_log_option="-Xloggc:${jvm_gc_log}"
fi

jvm_arg_line="-d64 ${jvm_direct_memory} ${jvm_min_heap} ${jvm_max_heap} ${jvm_gc_log_option} -ea"


db_relay_config_option="-db_relay_config ${db_relay_config}"
java_arg_line="${db_relay_config_option} ${config.file.option} ${cmdline.props.option} ${log4j.file.option}"


java -cp ${cp} ${jvm_arg_line} ${main_class} ${java_arg_line} $*