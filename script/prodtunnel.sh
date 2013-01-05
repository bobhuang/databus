#!/bin/bash

host=$1
rport=$2
lport=$3

if [ -z "$host" -o -z "$rport" ] ; then
  echo "USAGE: $0 host remote_port [local_port]"
fi

if [ -z "$lport" ] ; then
  lport=$rport
fi

used_ports_file=`mktemp used_ports.XXXXXX`
netstat -a --inet --numeric-ports | gawk '{print $4};' > $used_ports_file

function is_port_used() {
  port=$1
  grep ":${port}" $used_ports_file 2>&1 > /dev/null
}

function find_first_unused_port() {
  port=$1
  while is_port_used $port ; do
    port=$((port+1))
  done
  
  echo $port
}

real_lport=`find_first_unused_port $lport`
echo "using local port: $real_lport"
rm $used_ports_file

ssh -L "${real_lport}:localhost:$rport" -o "ProxyCommand /usr/bin/ssh eng-portal.corp.linkedin.com nc $host 22" $host