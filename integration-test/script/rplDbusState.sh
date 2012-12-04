#!/bin/bash 

#TOP_DIR=${PROJECT_TOP_DIR:-"./"}
SCRIPT_DIR=`dirname $0`
TOP_DIR=$SCRIPT_DIR/../../

if [ $# -ne 2 ] ; then
echo "Prints the state of thread in a rpldbus"
echo "usage: rplDbusState.sh host port "
exit -1
fi


host=$1
port=$2


cp=$HOME/.gradle/cache/log4j/log4j/jars/log4j-1.2.15.jar:$HOME/.gradle/cache/org.mysql/mysql-connector-java/jars/mysql-connector-java-5.1.14.jar:$TOP_DIR/build/databus-core-impl/classes/main:$TOP_DIR/build/databus-rpldbus-manager/classes/main


cmd="java -cp $cp com.linkedin.databus3.rpl_dbus_manager.RplDbusAdapter $host $port"
echo ABOUT TO RUN:  $cmd >&2


$cmd
