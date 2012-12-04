#!/bin/bash 


#usage
#gets rpl dbus  defaults file
# and returns port number of the Relay it connects too (always the same host)

if [ $# -ne 1 -o ! -e "$1" ]; then
	echo returns the port  of his RPLDBUS
	echo usage: rplDbus2relay  defaults-file
        exit  1
fi


rpldbus_file=$1
#echo rplhost = $rpldbus_file 


mysql_port=`mysql --defaults-file=$rpldbus_file --batch -uroot -e "show variables like 'port';" | grep port | cut -f2 `
echo $mysql_port
