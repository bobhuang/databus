#!/bin/bash

######################################
# MUST ADD THE FOLLOWING THREE LINES IN SUDOERS (sudo visudo) - scroll to the end of file and add it before the exit
# Allow user to stop, start mysql
# mgandhi ALL=(app)   NOPASSWD: ALL
# Defaults:mgandhi !requiretty
######################################

echo "deleting all rpl dbus threads..."

echo "stopping rpl dbus instances..."
mysqladmin --defaults-file=/export/apps/mysql-rpl-dbus/my.cnf -uroot shutdown
mysqladmin --defaults-file=/export/apps/mysql-rpl-dbus2/my.cnf -uroot shutdown
echo "rpl dbus stopped... now deleting thread files"

sudo -u app rm -f /export/apps/mysql-rpl-dbus/data/*.info
sudo -u app rm -f /export/apps/mysql-rpl-dbus/data/*.index
sudo -u app rm -f /export/apps/mysql-rpl-dbus/data/mysqld-relay-bin*.*
sudo -u app rm -f /export/apps/mysql-rpl-dbus2/data/*.info
sudo -u app rm -f /export/apps/mysql-rpl-dbus2/data/*.index
sudo -u app rm -f /export/apps/mysql-rpl-dbus2/data/mysqld-relay-bin*.*
echo "threads deleted.. now starting rpl dbus"

sudo -u app nohup mysqld_safe --defaults-file=/export/apps/mysql-rpl-dbus/my.cnf 1> mysql-rpl-dbus.out 2> mysql-rpl-dbus.out &
sudo -u app nohup mysqld_safe --defaults-file=/export/apps/mysql-rpl-dbus2/my.cnf 1> mysql-rpl-dbus2.out 2> mysql-rpl-dbus.out &
echo "done deleting all rpl dbus threads"
sleep 20

exit 0
