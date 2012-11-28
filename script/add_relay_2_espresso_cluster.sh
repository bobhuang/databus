#!/bin/bash

relay_list=$*

if [ -z "${relay_list}" ] ; then
   echo "$0: USAGE: $0 relay list"
   exit 1
fi

script_dir=`dirname $0`
source ${script_dir}/espresso_cluster_setup_settings.inc


echo "##### Setting up relay cluster ######"
for rlyhost in ${relay_list} ; do
  for instance in ${RELAY_INSTANCES} ; do
     rlyport=281${instance}
     relays="$relays $rlyhost:$rlyport"
  done
done

echo "##### Adding relays to Helix ######"
for rly in ${relays} ; do
  echo "Adding relay $rly to ${RELAY_CLUSTER_NAME}"
  if ${HELIX_CORE_PATH}/bin/helix-admin -zkSvr ${ZK_URL} --addNode  ${RELAY_CLUSTER_NAME} $rly ; then
    echo "Added."
  else
    echo "Failed."
    exit 2
  fi
done

echo "##### Adding relays to topology ######"
modtopo checkout --force
for rlyhost in ${relay_list} ; do
  for instance in ${RELAY_INSTANCES} ; do
     iname="databus3-relay-espresso-i0${instance}"
     echo "Adding ${iname} to $rlyhost"
     modtopo add-host --roles ${iname} -f EI $rlyhost
  done
  echo "Adding tags to ${rlyhost}"
  modtopo add-tag --hosts ${rlyhost} --app-id databus3-relay-espresso RELAY_DEV_FT_3
done
modtopo commit -m "Adding ${relay_list} to ${RELAY_CLUSTER_NAME}"
