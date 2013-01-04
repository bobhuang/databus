#!/bin/bash

script_dir=`dirname $0`
$script_dir/run-cp-serializer.sh -S com.linkedin.events.mbrpic.MemberPicture -C $script_dir/../conf/cp3-bootstrap-producer-mbrpic.properties -f cp3 $*
