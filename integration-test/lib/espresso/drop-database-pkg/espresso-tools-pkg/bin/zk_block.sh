#!/bin/bash -x


if [ $# -eq 4  -a "$1" == "block" ]
then
  #set parameters
  cmd=$1
  zkHost=$2
  zkPort=$3
  clientHost=$4
elif [ $# -eq 2 -a "$1" == "unblock" ]
then
  cmd=$1
  clientHost=$2
else
  echo 'usage:'
  echo "  zk_ctrl.sh block zkHost zkPort clientHost"
  echo "  zk_ctrl.sh unblock clientHost"
  exit 1
fi

echo zk=$zkHost:$zkPort,cl=$clientHost,cmd=$cmd


if [ "$cmd" == "block" ] 
then
	echo running ssh $clientHost "sudo /sbin/iptables -d $zkHost -A OUTPUT  -p TCP --dport=$zkPort -j DROP"
	ssh $clientHost "sudo /sbin/iptables -d $zkHost -A OUTPUT  -p TCP --dport=$zkPort -j DROP"
	exit $?
fi

if [ "$cmd" == "unblock" ] 
then
	echo running ssh $clientHost "sudo /sbin/iptables -D OUTPUT 1"
	ssh $clientHost "sudo /sbin/iptables -D OUTPUT 1"
	exit $?
fi

