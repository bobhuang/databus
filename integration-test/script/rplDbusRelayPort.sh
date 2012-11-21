#!/bin/bash 


#usage
#gets rpl dbus  defaults file
# and returns port number of the Relay it connects too (always the same host)

if [ $# -ne 1 -o ! -e "$1" ]; then
	echo usage: rplDbus2relay  defaults-file
        exit  1
fi


rpldbus_file=$1
#echo rplhost = $rpldbus_file 


relay_port=`mysql --defaults-file=$rpldbus_file --batch -uroot -e  "select value from mysql.rpl_dbus_config where prop_key='databus2.mysql.port'\G;" | grep value | cut -f2 -d' '` 
echo $relay_port
