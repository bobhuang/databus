#!/bin/bash

echo "######## Installing glu agent ##############"
sudo yum -y install glu-agent

echo "######## Stopping mysql ##############"
sudo /etc/init.d/mysql stop

echo "######## Killing any running mysql instances ###########" 
ps -ef | grep mysqld | awk '{print $2}' | xargs sudo kill -9

echo "########## Uninstalling mysql packages #############"
rpm -qa | grep mysql | xargs -ipackage sudo rpm  -e package --nodeps

echo "######## Downloading latest mysql package #########"
sudo rpm -U http://apachev-ld.linkedin.biz/rpms/MySQL-bundle-Linkedin-5.5.8-latest.x86_64.rpm

server_ip=`ifconfig -a | grep 'inet addr' | grep -v '127\.0\.0\.1' | sed -e 's/.*inet addr:\([^ ]*\).*/\1/'`
if [ -z "${server_ip}" ] ; then
  echo "$0: failed to determine server id"
  exit 1
fi

export default_server_id=`perl -e 'if ($ARGV[0] =~ /[\d\.]+\.(\d+)\.(\d+)/) {print $1*256 + $2;}' ${server_ip}`
if [ -z "${default_server_id}" ]; then
  echo "$0: unable to calculate base server_id"
  exit 1
fi 

export MYSQL_MASTER_HOST=eat1-app103.corp.linkedin.com
export MYSQL_MASTER_PORT=3306

for i in 01 02 03 04 05 06 07 08 09 ; do
  export MYSQL_SLAVE_PORT="280$i"
  export MYSQL_SLAVE_SERVER_ID=$((default_server_id + MYSQL_SLAVE_PORT * 65536))
  export MYSQL_SLAVE_HOME_DIR="/export/apps/mysql-${MYSQL_SLAVE_SERVER_ID}"
  export MYSQL_SLAVE_HOME_DIR_WIPE="n"
  export RELAY_TCP_PORT="290$i"
  
  echo "######## Setting up slave $i on port ${MYSQL_SLAVE_PORT} serverid ${MYSQL_SLAVE_SERVER_ID} #########"
  sudo -E setup-rpl-dbus-slave
  
  echo "######## Granting privileges for instance $i #########"
  if mysql --defaults-file=${MYSQL_SLAVE_HOME_DIR}/my.cnf -uroot -e "grant all on *.* to root@'localhost' identified by '';" ; then
    echo -n     
  else
     echo "$0: grant 1 failed."
  fi
  if mysql --defaults-file=${MYSQL_SLAVE_HOME_DIR}/my.cnf -uroot -e "grant all on *.* to root@'%' identified by '';" ; then
     echo -n
  else
     echo "$0: grant 2 failed."
  fi
  if mysql --defaults-file=${MYSQL_SLAVE_HOME_DIR}/my.cnf -uroot -e "grant all on *.* to espresso@'localhost' identified by 'espresso';" ; then
     echo -n
  else
     echo "$0: grant 3 failed."
  fi
  if mysql --defaults-file=${MYSQL_SLAVE_HOME_DIR}/my.cnf -uroot -e "grant all on *.* to rplespresso@'%' identified by 'espresso';" ; then
     echo -n
  else
     echo "$0: grant 4 failed."
  fi
  if mysql --defaults-file=${MYSQL_SLAVE_HOME_DIR}/my.cnf -uroot -e "grant all on *.* to espresso@'%' identified by 'espresso';" ; then
     echo -n
  else
     echo "$0: grant 5 failed."
  fi
  
  echo "######## Configuring rpl_dbus instance $i #########"
  if mysql --defaults-file=${MYSQL_SLAVE_HOME_DIR}/my.cnf -uroot -e "insert into mysql.rpl_dbus_config values ('databus2.mysql.port','${RELAY_TCP_PORT}') on duplicate key update value='${RELAY_TCP_PORT}';" ; then
    echo -n     
  else
     echo "$0: configuring relay port failed."
  fi
  if mysql --defaults-file=${MYSQL_SLAVE_HOME_DIR}/my.cnf -uroot -e "insert into mysql.rpl_dbus_config values ('databus2.mysql.readTimeout','30') on duplicate key update value='30';" ; then
    echo -n     
  else
     echo "$0: configuring read timeout failed."
  fi
  if mysql --defaults-file=${MYSQL_SLAVE_HOME_DIR}/my.cnf -uroot -e "insert into mysql.rpl_dbus_config values ('databus2.mysql.writeTimeout','30') on duplicate key update value='30';" ; then
    echo -n     
  else
     echo "$0: configuring write timeout failed."
  fi
  if mysql --defaults-file=${MYSQL_SLAVE_HOME_DIR}/my.cnf -uroot -e "insert into mysql.rpl_dbus_config values ('databus2.mysql.connect_retry_wait','30') on duplicate key update prop_value='30';" ; then
    echo -n     
  else
     echo "$0: configuring connect_retry_wait failed."
  fi
  if mysql --defaults-file=${MYSQL_SLAVE_HOME_DIR}/my.cnf -uroot -e "set global rpl_dbus_request_config_reload=1;" ; then
    echo -n     
  else
     echo "$0: flushing of the configuration failed."
  fi
  
done


