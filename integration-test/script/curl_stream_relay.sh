#!/bin/bash 

if [ ! $# -eq 3 ]; then
	echo "Usage: curl_stream_realy.sh <port> <dbName> <partititon>"
        exit
fi
host=localhost
port=$1
dbName=$2
dbPart=$3


curl -g -s 'localhost:'${port}'/stream?subs=[{"physicalSource":{"uri":"databus:physical-source:ANY"},"physicalPartition":{"id":'${dbPart}',"name":"'${dbName}'"},"logicalPartition":{"source":{"id":-1,"name":"*"},"id":-1}}]&streamFromLatestScn=false&checkPoint={"windowOffset":-1,"bootstrap_start_scn":-1,"snapshot_offset":-1,"windowScn":-1,"consumption_mode":"ONLINE_CONSUMPTION"}&output=json&size=1024000'
