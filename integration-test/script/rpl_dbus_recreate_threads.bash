#!/bin/bash
######################################
# MUST ADD THE FOLLOWING THREE LINES IN SUDOERS (sudo visudo) - scroll to the end of file and add it before the exit
# Allow user to stop, start mysql
# mgandhi ALL=(app)   NOPASSWD: ALL
# Defaults:mgandhi !requiretty
######################################

SCRIPT_DIR=`dirname $0`
$SCRIPT_DIR/rpl_dbus_delete_threads.bash

echo "Recreating rpl dbus threads ..."
mysql --defaults-file=/export/apps/mysql-rpl-dbus/my.cnf -uroot -e"set global Li_rpl_slave_name=\"\";stop slave; change master to master_host='127.0.0.1',master_port=14100,master_user='espresso',master_password='espresso';"
mysql --defaults-file=/export/apps/mysql-rpl-dbus2/my.cnf -uroot -e"set global Li_rpl_slave_name=\"\";stop slave; change master to master_host='127.0.0.1',master_port=3306,master_user='espresso',master_password='espresso';" 1> rpl1.out 2> rpl1.out
echo "deleting all rpl dbus threads..."

echo "done recreating rpl dbus threads"
exit 0
