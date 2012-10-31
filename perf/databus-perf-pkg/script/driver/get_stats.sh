#!/bin/bash

exp=$1

if [ -z "$exp" ] ; then
	echo "USAGE: $0 experiment" ;
	exit 1
fi

./get_exp_metric.sh $exp- 'events ' $exp-events.txt
./get_exp_metric.sh $exp- 'events/sec' $exp-eps.txt
./get_exp_metric.sh $exp- 'bytes, MB/s' $exp-thruput.txt
./get_exp_metric.sh $exp- 'avg lag' $exp-lag.txt
