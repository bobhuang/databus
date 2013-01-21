#!/bin/bash

script_dir=`dirname $0`
$script_dir/run-cp-serializer.sh -S com.linkedin.events.sas.Creatives,com.linkedin.events.sas.Campaigns  -C $script_dir/../conf/cp3-bootstrap-producer-sas.properties -f cp3 $*
