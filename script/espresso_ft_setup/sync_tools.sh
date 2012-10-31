#!/bin/bash

script_dir=`dirname $0`
source ${script_dir}/espresso_cluster_setup_settings.inc

export RSYNC_SSH="ssh -o StrictHostKeyChecking=no"
RSYNC="rsync -ruz --exclude '.svn/**/*' --exclude 'databus2-trunk-cfg/**/*' --exclude .svn --exclude databus2-trunk-cfg "

echo "Initiating asynchronous rsync. Please wait until the following hosts are synced: ${RELAY_HOSTS[@]} ${SN_HOSTS[@]}"

for r in "${RELAY_HOSTS[@]}" ; do
	$RSYNC . $r:${RELAY_CLUSTER_NAME} && echo "$r synched" &
done

for sn in "${SN_HOSTS[@]}" ; do
	$RSYNC . $sn:${ESPRESSO_CLUSTER_NAME} && echo "$sn synched" &
done
