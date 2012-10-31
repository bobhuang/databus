#!/bin/bash 

#currently sets relays logging for FT_2 cluster 
#later may be changed to have parameters

level=DEBUG
if [ $# -eq 1 ]
then
	level=$1
	echo setting level $level
fi

for i in 64 65 66 141 ; do

echo curl "http://eat1-app$i.corp.linkedin.com:11139/jmx/invoke?operation=setLoggerLevel&objectname=databus3-relay-espresso.log4j%3Aname%3Dadmin&value0=com.linkedin.databus.container.request.tcp&type0=java.lang.String&value1=${level}&type1=java.lang.String"
curl -s "http://eat1-app$i.corp.linkedin.com:11139/jmx/invoke?operation=setLoggerLevel&objectname=databus3-relay-espresso.log4j%3Aname%3Dadmin&value0=com.linkedin.databus.container.request.tcp&type0=java.lang.String&value1=${level}&type1=java.lang.String" > /dev/null

if [ $? != 0 ] 
then
echo ERROR command to relay eat1-app$i.corp failed. res = $?
fi

done
