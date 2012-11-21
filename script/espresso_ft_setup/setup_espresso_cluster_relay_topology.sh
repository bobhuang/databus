#!/bin/bash

script_dir=`dirname $0`
source ${script_dir}/espresso_cluster_setup_settings.inc
source ${script_dir}/common_funcs.inc

modtopo checkout --force
for r in "${RELAY_HOSTS[@]}" ; do
	for i in "${RELAY_INSTANCES[@]}" ; do
		modtopo add-host --roles databus3-relay-espresso-i0$i -f ${CLUSTER_FABRIC} $r
	done
        modtopo add-tag --hosts $r --app-id databus3-relay-espresso databus2,espresso,espresso-relay,backend,ENG:DDS,${RELAY_CLUSTER_NAME}
done
modtopo commit -m "Adding ${RELAY_CLUSTER_NAME} relays"
