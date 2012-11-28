#!/bin/bash
ORIGINAL_PATH="$(pwd)"
cd "$(dirname $0)"

# Absolute Path to this dir
export SCRIPT_PATH="$(pwd)"
cd "${ORIGINAL_PATH}"

# remove checkpoint
if [ "$VIEW_ROOT" == "" ]; then
  VIEW_ROOT=$SCRIPT_PATH/../..
fi
rm -f $VIEW_ROOT/databus2-checkpoints/*
rm -f $VIEW_ROOT/bootstrap-checkpoints/*
rm -f $VIEW_ROOT/integratedconsumer-checkpoints/*
rm -f $VIEW_ROOT/bfclient-checkpoints/*
rm -f $VIEW_ROOT/liarclient-checkpoints/*
rm -f $VIEW_ROOT/integration-test/integ/databus2-checkpoints/*
rm -f $VIEW_ROOT/integration-test/integ/bootstrap-checkpoints/*
rm -f $VIEW_ROOT/integration-test/integ/integratedconsumer-checkpoints/*
rm -f $VIEW_ROOT/integration-test/bfclient-checkpoints/*
rm -f $VIEW_ROOT/integration-test/liarclient-checkpoints/*
rm -f ./databus2-checkpoints/*
rm -f ./bootstrap-checkpoints/*
rm -f ./integratedconsumer-checkpoints/*
rm -f ./bfclient-checkpoints/*
rm -f ./liarclient-checkpoints/*

# kill all the lock processes
mysql -uroot -s -e 'show processlist' | grep "LOCK TABLES" | awk '{print $1}'  | while read f; do mysqladmin -uroot kill $f; done
mysql -uroot -s -e 'show processlist' | grep "Waiting for table" | awk '{print $1}'  | while read f; do mysqladmin -uroot kill $f; done
mysql -uroot -s -e 'show processlist' | grep "select sleep" | awk '{print $1}'  | while read f; do mysqladmin -uroot kill $f; done

# recreate user and database
$VIEW_ROOT/database/databus2-bootstrap/createUser/cr_databus2.sh

# recreate table
# ant -f $VIEW_ROOT/database/databus2-bootstrap/build.xml db.reinit
$VIEW_ROOT/database/databus2-bootstrap/bootstrap_db_reinit.sh

echo "DBReinit Complete"
