#!/bin/bash
## clean up all non-standard installs of rpl dbus from your machine. 
##     stop all non standard instances
##     delete the config and data folders e.g. rm -rf /export/apps/mysql-rpl-dbus/*

## install the latest rpm 
#rpm -U http://apachev-ld.linkedin.biz/rpms/MySQL-bundle-Linkedin-5.5.8-rel${1}.x86_64.rpm
#/etc/init.d/mysql start
## setup the master...choose all defaults here
export MYSQL_MASTER_PORT=3306
export MYSQL_MASTER_HOST="127.0.0.1"
export MYSQL_MASTER_HOME_DIR="/export/apps/mysql"
export MYSQL_MASTER_HOME_DIR_WIPE="Y"
export MYSQL_MASTER_SERVER_ID="3306"
## to be wiped out without being prompted, N for the install to abort if it does exist prior.
setup-master
export MYSQL_MASTER_PORT=14100
export MYSQL_MASTER_HOST="127.0.0.1"
export MYSQL_MASTER_HOME_DIR="/export/apps/mysql-master2"
export MYSQL_MASTER_HOME_DIR_WIPE="Y"
export MYSQL_MASTER_SERVER_ID="14100"
## to be wiped out without being prompted, N for the install to abort if it does exist prior.
setup-master

# setup the first slave. For config dir use /export/apps/mysql-rpl-dbus instead of the default
export MYSQL_MASTER_PORT=3306
export MYSQL_SLAVE_PORT=29000
export MYSQL_MASTER_HOST="127.0.0.1"
export MYSQL_SLAVE_HOME_DIR="/export/apps/mysql-rpl-dbus"
export MYSQL_SLAVE_HOME_DIR_WIPE="Y"
export MYSQL_SLAVE_SERVER_ID="1900550039"
setup-rpl-dbus-slave
# setup the second slave. For config dir use /export/apps/mysql-rpl-dbus2 instead of the default
export MYSQL_MASTER_PORT=3306
export MYSQL_SLAVE_PORT=29001
export MYSQL_MASTER_HOST="127.0.0.1"
export MYSQL_SLAVE_HOME_DIR="/export/apps/mysql-rpl-dbus2"
export MYSQL_SLAVE_HOME_DIR_WIPE="Y"
export MYSQL_SLAVE_SERVER_ID="1900615575"
setup-rpl-dbus-slave


#make user espresso on second master and give all permissions.
/usr/bin/mysql --defaults-file=/etc/my.cnf -uroot -e  "grant all on *.* to rplespresso@localhost identified by 'espresso';"
/usr/bin/mysql --defaults-file=/export/apps/mysql-master2/my.cnf -uroot -e  "grant all on *.* to rplespresso@localhost identified by 'espresso';"
/usr/bin/mysql --defaults-file=/export/apps/mysql-rpl-dbus/my.cnf -uroot -e  "grant all on *.* to rplespresso@localhost identified by 'espresso';"
/usr/bin/mysql --defaults-file=/export/apps/mysql-rpl-dbus2/my.cnf -uroot -e  "grant all on *.* to rplespresso@localhost identified by 'espresso';"

# for the first slave setup the second slave thread
/usr/bin/mysql --defaults-file=/export/apps/mysql-rpl-dbus/my.cnf -uroot -e "set global li_rpl_slave_name='default2';"
/usr/bin/mysql --defaults-file=/export/apps/mysql-rpl-dbus/my.cnf -uroot -e "stop slave;"
/usr/bin/mysql --defaults-file=/export/apps/mysql-rpl-dbus/my.cnf -uroot -e "change master to master_host='127.0.0.1',master_port=14100,master_user='rplespresso',master_password='espresso';"
# for the second slave change the relay port to 11151
/usr/bin/mysql --defaults-file=/export/apps/mysql-rpl-dbus2/my.cnf -uroot mysql -e "update rpl_dbus_config SET value='11151' where prop_key='databus2.mysql.port';"
/usr/bin/mysql --defaults-file=/export/apps/mysql-rpl-dbus2/my.cnf -uroot -e "set global rpl_dbus_request_config_reload=1;"
/usr/bin/mysql --defaults-file=/export/apps/mysql-rpl-dbus2/my.cnf -uroot -e "stop slave;"
/usr/bin/mysql --defaults-file=/export/apps/mysql-rpl-dbus2/my.cnf -uroot -e "start slave;"
# for the second slave setup the second slave thread
/usr/bin/mysql --defaults-file=/export/apps/mysql-rpl-dbus2/my.cnf -uroot -e "set global li_rpl_slave_name='default2';"
/usr/bin/mysql --defaults-file=/export/apps/mysql-rpl-dbus2/my.cnf -uroot -e "stop slave;"
/usr/bin/mysql --defaults-file=/export/apps/mysql-rpl-dbus2/my.cnf -uroot -e "change master to master_host='127.0.0.1',master_port=14100,master_user='rplespresso',master_password='espresso';"
