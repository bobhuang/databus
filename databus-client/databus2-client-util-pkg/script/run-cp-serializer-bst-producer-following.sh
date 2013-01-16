#!/bin/bash

script_dir=`dirname $0`
$script_dir/run-cp-serializer.sh -S com.linkedin.events.following.Following -C $script_dir/../conf/cp3-bootstrap-producer-following.properties -f cp3 $*
