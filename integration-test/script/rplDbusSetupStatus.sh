#!/bin/bash 


SCRIPT_DIR=`dirname $0`
TOP_DIR=$SCRIPT_DIR/../../

#usage
#gets rpl dbus  defaults file
# and returns port number of the Relay it connects too (always the same host)

if [ $# -lt 1 ]; then
	echo "prints out current state of rpl dbus (assuming all on same host)"
	echo usage: rplDbus2relay  [defaults-file1 ...]
        exit  1
fi


while [ $# -ne 0 ]; do 
	rpldbus_file=$1

	rplDbusPort=`$SCRIPT_DIR/rplDbusOwnPort.sh $rpldbus_file`
	rplDbusRelayPort=`$SCRIPT_DIR/rplDbusRelayPort.sh $rpldbus_file` 
	rplDbusHost=localhost

        echo '-----------------------------------------------------------------------------'
	echo "RPLDBUS($rplDbusHost:$rplDbusPort) connected to Relay($rplDbusHost:$rplDbusRelayPort):" 
	$SCRIPT_DIR/rplDbusState.sh  $rplDbusHost $rplDbusPort

	echo '-----------------------------------------------------------------------------' 
	shift
done
