#!/bin/sh
PRODUCER_OUT=$1
CONSUMER_OUT=$2

#cat ../databus2-trunk/databus-profile-relay/run/run.out  | grep "Produce:" | sed 's/^.*Produce://' > produce.out
cat $PRODUCER_OUT | grep "Produce:" | sed 's/^.*Produce://' > produce.out
#cat ../databus2-trunk/databus-profile-client/run/consume.out | grep "Consume:"  | grep "INFO Con"  | sed 's/^.*Consume://' | sed 's/ (com.linkedin.databus.client.LoggingConsumer)//' > consume.out
cat $CONSUMER_OUT | grep "Consume:"  | grep "INFO Con"  | sed 's/^.*Consume://' | sed 's/ (com.linkedin.databus.client.LoggingConsumer)//' > consume.out
diff produce.out consume.out
