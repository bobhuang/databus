#!/bin/bash

script_dir=`dirname $0`
$script_dir/run-cp-serializer.sh -S com.linkedin.events.eos.MemberEmails -C $script_dir/../conf/cp3-bootstrap-producer-eos.properties -f cp3 $*
