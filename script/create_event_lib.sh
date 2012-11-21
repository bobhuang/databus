#!/bin/bash

lib_name=$1

if [ -z "${lib_name}" ] ; then
	echo "usage: $0 lib_name"
	exit 1
fi

root_dir=`dirname $0`/..
events_proj_dir=${root_dir}/databus-events

mkdir -p ${events_proj_dir}/databus-events-${lib_name}/src/main/java
cp ${events_proj_dir}/databus-events-bizfollow/build.gradle ${events_proj_dir}/databus-events-${lib_name}/
