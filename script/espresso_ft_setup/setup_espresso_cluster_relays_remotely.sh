#!/bin/bash

script_dir=`dirname $0`
source ${script_dir}/espresso_cluster_setup_settings.inc

for r in "${RELAY_HOSTS[@]}" ; do
  clear
  echo "SETTING UP RPL_DBUS ON $r."
  echo "USE YOUR KERBEROS PASSWORD IF ASKED FOR A PASSWORD!"
  echo "============================"
  ssh -t $r "export PATH=/bin:/sbin:/usr/bin:/usr/sbin:\$PATH && ${RELAY_CLUSTER_NAME}/setup_espresso_cluster_relay.sh"
  echo "============================"
  echo "Setup complete. If you saw any error, please exit now and fix them manually. Continue with next host? (Y/N)"
  read cont
  if [ "$cont" != "Y" -a "$cont" != "yes" -a "$cont" != "y" -a "$cont" != "YES" ] ; then
	echo "Aborting..."
	exit 1
  fi 
done


