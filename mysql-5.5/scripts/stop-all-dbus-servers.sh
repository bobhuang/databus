#! /bin/sh

#echo "Killing servers"

for f in $MYSQLTEST_VARDIR/rpl_dbus.pid*
do
  if [ -f "$f" ]
  then
    pid=`cat $f`
    if [ -n "$pid" ]
    then
      kill $pid 2>/dev/null
      sleep 1 
      kill -9 $pid 2>/dev/null
      rm -f $f
    fi  
  fi
done

#echo "RPL_DBUS_PORTS=$RPL_DBUS_PORTS"

for p in $RPL_DBUS_PORTS
do
  pid=`netstat --tcp -apn 2>/dev/null |grep 127.0.0.1:$p | grep LISTEN | awk '{ split($7,a,"/"); print a[1];}'`
  kill $pid 2>/dev/null
  sleep 1
  kill -9 $pid 2>/dev/null
  while true
  do
    out=`netstat --tcp -apn 2>/dev/null |grep 127.0.0.1:$p`
    #echo "out=$out"
    if [ -n "$out" ]
    then
      sleep 2
    else
      break  
    fi  
  done
done


