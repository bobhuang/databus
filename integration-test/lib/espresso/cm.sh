#!/bin/bash
SCRIPT=`readlink -f $0`
# Absolute path this script is in, thus /home/user/bin
SCRIPTPATH=`dirname $SCRIPT`
cur_dir=$SCRIPTPATH
export ESPRESSO_HOME=${cur_dir}
echo "Calling cluster admin .."
cd ${ESPRESSO_HOME}/helix-core-pkg/bin
echo $@
./helix-admin $@
echo "cluster admin done" 

