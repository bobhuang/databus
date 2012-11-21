#!/bin/bash

fabric=$1
service=$2

script_dir=`dirname $0`

hosts=`glulist -f $fabric -s $service`
$script_dir/run_scrapers.sh $hosts