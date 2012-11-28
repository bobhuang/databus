#!/bin/bash

expid=$1
relay=$2
source=$3
baseport=$4
buffer=$5
poll=$6
duration=$7
ts=$8
clientn=$9
shift 9
partbase=$1
partgr=$2

echo partition=$part

script_dir=`dirname $0`

date

for i in `seq 1 $clientn` ; do
  lport=$((baseport+i))

  if [ "$partbase" != "" -a "$partbase" != "0" ] ; then
        partid=$((clientn * partgr + i - 1))
	modpart="$partbase:[$partid]"
	echo "modpart=$modpart"
  fi

  ${script_dir}/run_1_consumer.sh $expid $relay $source $lport $buffer $poll $duration $ts $modpart & 
done

