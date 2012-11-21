#!/bin/bash

SCRIPT_DIR=`dirname $0`
echo $1
if [ $1 = "11140" ];
then
    echo "Stopping threads on rpl connected to relay 11140 ..."
    mysql --defaults-file=/export/apps/mysql-rpl-dbus/my.cnf -uroot -e"set global Li_rpl_slave_name=\"\";stop slave; set global Li_rpl_slave_name="default2";stop slave; "

elif [ $1 = "11150" ];
then
    echo "Stopping threads on rpl connected to relay 11150 ..."
    mysql --defaults-file=/export/apps/mysql-rpl-dbus2/my.cnf -uroot -e"set global Li_rpl_slave_name=\"\";stop slave; set global Li_rpl_slave_name="default2";stop slave; "
else
    echo "No rpl instance found!!"
    exit -2
fi
exit 0
