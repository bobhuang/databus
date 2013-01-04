#!/bin/bash

script_dir=`dirname $0`
$script_dir/run-cp-serializer.sh -S com.linkedin.events.entitlements.Entitlements -C $script_dir/../conf/cp3-bootstrap-producer-entitlements.properties -f cp3 $*
