#!/bin/bash

script_dir=`dirname $0`
source ${script_dir}/espresso_cluster_setup_settings.inc
source ${script_dir}/common_funcs.inc

if [ ! -d ${ESPRESSO_TOOLS_PATH} ] ; then
  download_etools
fi

echo "##### Setting up relay cluster ######"
relays=""
for rlyhost in "${RELAY_HOSTS[@]}" ; do
  for i in "${RELAY_INSTANCES[@]}" ; do
     rlyport=${RELAY_HTTP_PORT_PREFIX}${i}
     relays="$relays $rlyhost:$rlyport"
  done
done

${ESPRESSO_TOOLS_PATH}/bin/add_relays_2_cm.sh ${HELIX_ADMIN_URL} ${RELAY_CLUSTER_NAME} "$relays" ${SUPER_CLUSTER_NAME}

