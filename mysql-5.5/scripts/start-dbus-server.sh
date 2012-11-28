#! /bin/bash

min_ver=2.6
PYTHON=/usr/bin/python

for p in /usr/bin/python*.*[0-9]
do
   $p <<eot
import sys
def version_cmp(v1, v2):
       parts1, parts2 = [map(int, v.split('.')) for v in [v1, v2]]
       parts1, parts2 = zip(*map(lambda p1,p2: (p1 or 0, p2 or 0), parts1, parts2))
       return cmp(parts1, parts2)

sys.exit(version_cmp(sys.version.split(' ')[0],'$min_ver') >=0)
eot
  if [ $? != 0 ]
  then
    PYTHON=$p
    break
  fi  
done



echo "Starting dbus server, port $RPL_DBUS_PORT, python=$PYTHON"
$PYTHON $MYSQL_TEST_DIR/suite/rpl_dbus/scripts/dbus-server.py --port=$RPL_DBUS_PORT --pid-file=$RPL_DBUS_PID > $RPL_DBUS_LOG & 
