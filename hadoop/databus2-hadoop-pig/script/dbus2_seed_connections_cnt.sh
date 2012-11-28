#!/bin/bash

script_dir=`dirname $0`
source ${script_dir}/setup.inc
source ${script_dir}/hadoop-setup.inc

SRCID=602
SRC_TAB_NAME=tab_${SRCID}
source=$1

user=${BOOTSTRAP_USER:-bootstrap}
pass=${BOOTSTRAP_PASSWORD:-bootstrap}

# main()

if (mysql -u${user} -p${pass} -e "show databases;" | grep bootstrap) ; then
  echo "Bootstrap DB found!"
else
  read -n 1 -p "Create a new bootstrap DB (Y/N)" do_create_db
  if [ "${do_create_db}" == "Y" -o "${do_create_db}" == "y" ] ; then
          bin/databus2-bootstrap/bootstrap_db_reinit.sh
  fi
fi

if (mysql -u${user} -p${pass} -Dbootstrap -e "show tables like '${SRC_TAB_NAME}';" | grep ${SRC_TAB_NAME}) ; then
  echo "Dropping ${SRC_TAB_NAME} table ... "
  mysql -u${user} -p${pass} -Dbootstrap -e "DROP TABLE ${SRC_TAB_NAME};"
fi

echo "Creating table ${SRC_TAB_NAME} ..."
bin/dbus2_bootstrapdb_ctrl.sh -a CREATE_TAB_TABLE -S ${SRCID} -c conf/sources-conn-ela4.json

echo "Removing unique srckey index from ${SRC_TAB_NAME} for better performance ..."
mysql -u${user} -p${pass} -Dbootstrap -e "alter table ${SRC_TAB_NAME} drop index srckey;"

echo "Loading data"
time bin/run-avro-seeder-meta.sh ela4 connections_cnt

# HACK
echo "Removing bootstrap_loginfo entries so that SEED finalization does not bark"
mysql -u${user} -p${pass} -Dbootstrap -e "delete from bootstrap_loginfo;"

echo "Creating unique srckey index on ${SRC_TAB_NAME} ..."
time mysql -u${user} -p${pass} -Dbootstrap -e "CREATE UNIQUE INDEX srckey ON ${SRC_TAB_NAME} (srckey );" 

echo "Number of rows seeded"
time mysql -u${user} -p${pass} -Dbootstrap -e "SELECT count(*) FROM ${SRC_TAB_NAME};"
