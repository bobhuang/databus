#! /bin/bash
printf "1234" > $MYSQLTEST_VARDIR/mysqld.2/data/rpl_dbus.info
RPL_DBUS_PORTS="12178 12179"
. $MYSQL_TEST_DIR/suite/rpl_dbus/scripts/stop-all-dbus-servers.sh

RPL_DBUS_PORT=12178
RPL_DBUS_PID=$MYSQLTEST_VARDIR/rpl_dbus.pid
RPL_DBUS_LOG=$MYSQLTEST_VARDIR/rpl_dbus.log
. $MYSQL_TEST_DIR/suite/rpl_dbus/scripts/start-dbus-server.sh 
RPL_DBUS_PORT=12179
RPL_DBUS_LOG=$MYSQLTEST_VARDIR/rpl_dbus.log.1
RPL_DBUS_PID=$MYSQLTEST_VARDIR/rpl_dbus.pid.1
. $MYSQL_TEST_DIR/suite/rpl_dbus/scripts/start-dbus-server.sh
