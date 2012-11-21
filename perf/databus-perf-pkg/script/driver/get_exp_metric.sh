#!/bin/bash

experiment=$1
metric=$2
file=$3

script_dir=`dirname $0`
source ${script_dir}/common.inc

find ${data_dir} -iname "result-*$experiment*" | xargs grep "$metric" | sed -e 's/.*-g\([0-9]*\)c\([0-9]*\)p\([0-9]*\)[-_].*: *\([0-9.]*\).*/\1,\2,\3,\4/'  | tee ${data_dir}/$file
