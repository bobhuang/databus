#!/bin/bash

script_dir=`dirname $0`
source ${script_dir}/espresso_cluster_setup_settings.inc
source ${script_dir}/common_funcs.inc

echo "######## Installing glu agent ##############"
sudo yum -y install glu-agent

echo "######## Stopping mysql ##############"
sudo /etc/init.d/mysql stop

echo "######## Killing any running mysql instances ###########" 
ps -ef | grep mysqld | awk '{print $2}' | xargs sudo kill -9

echo "########## Uninstalling mysql packages #############"
rpm -qa | grep mysql | xargs -ipackage sudo rpm  -e package --nodeps

echo "######## Downloading latest mysql package #########"
ulimit -u 16357
sudo rpm -U http://apachev-ld.linkedin.biz/rpms/MySQL-bundle-Linkedin-5.5.8-latest.x86_64.rpm

discover_ip

#export MYSQL_MASTER_HOST=li.null
export MYSQL_MASTER_HOST=${DEFAULT_MYSQL_MASTER_HOST}
export MYSQL_MASTER_PORT=${DEFAULT_MYSQL_MASTER_PORT}
export MYSQL_SLAVE_HOME_DIR_WIPE=Y

for i in "${RELAY_INSTANCES[@]}" ; do
  export MYSQL_SLAVE_PORT="280$i"
  export MYSQL_SLAVE_SERVER_ID=`mysql_instance_id $MYSQL_SLAVE_PORT`
  export MYSQL_SLAVE_HOME_DIR="/export/apps/mysql-${MYSQL_SLAVE_SERVER_ID}"
  export RELAY_TCP_PORT="${RELAY_TCP_PORT_PREFIX}$i"
  
  echo "######## Setting up slave $i on port ${MYSQL_SLAVE_PORT} serverid ${MYSQL_SLAVE_SERVER_ID} #########"
  sudo -E ${script_dir}/setup-rpl-dbus.sh

  echo "####### Turning off replication thread ##########"
  if mysql --defaults-file=${MYSQL_SLAVE_HOME_DIR}/my.cnf -uroot -e "stop slave;change master to master_host='li.null';" ; then
    echo -n
  else
    echo "$0: turning off of replication thread failed"
  fi

  echo "####### Creating rpl_dbus config table  ##########"
  if mysql --defaults-file=${MYSQL_SLAVE_HOME_DIR}/my.cnf -uroot -e "create table if not exists mysql.rpl_dbus_config(prop_key varchar(128) not null primary key, value varchar(50) not null) engine=myisam;" ; then
    echo -n
  else
    echo "$0: creating rpl_dbus config table failed failed"
  fi
  
  echo "######## Granting privileges for instance $i #########"
  if mysql --defaults-file=${MYSQL_SLAVE_HOME_DIR}/my.cnf -uroot -e "grant all on *.* to root@'localhost' identified by '${RPL_DBUS_ROOT_PWD}';" ; then
    echo -n     
  else
     echo "$0: grant 1 failed."
  fi
  if mysql --defaults-file=${MYSQL_SLAVE_HOME_DIR}/my.cnf -uroot -e "grant all on *.* to root@'%' identified by '${RPL_DBUS_ROOT_PWD}';" ; then
     echo -n
  else
     echo "$0: grant 2 failed."
  fi
  if mysql --defaults-file=${MYSQL_SLAVE_HOME_DIR}/my.cnf -uroot -e "grant all on *.* to ${SN_MYSQL_USER_NAME}@'localhost' identified by '${SN_MYSQL_USER_PWD}';" ; then
     echo -n
  else
     echo "$0: grant 3 failed."
  fi
  if mysql --defaults-file=${MYSQL_SLAVE_HOME_DIR}/my.cnf -uroot -e "grant all on *.* to ${RPL_DBUS_USER_NAME}@'%' identified by '${RPL_DBUS_USER_PWD}';" ; then
     echo -n
  else
     echo "$0: grant 4 failed."
  fi
  if mysql --defaults-file=${MYSQL_SLAVE_HOME_DIR}/my.cnf -uroot -e "grant all on *.* to ${SN_MYSQL_USER_NAME}@'%' identified by '${SN_MYSQL_USER_PWD}';" ; then
     echo -n
  else
     echo "$0: grant 5 failed."
  fi
  if mysql --defaults-file=${MYSQL_SLAVE_HOME_DIR}/my.cnf -uroot -e "grant all on *.* to ${RPL_DBUS_USER_NAME}@'localhost' identified by '${RPL_DBUS_USER_PWD}';" ; then
     echo -n
  else
     echo "$0: grant 6 failed."
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
  if mysql --defaults-file=${MYSQL_SLAVE_HOME_DIR}/my.cnf -uroot -e "insert into mysql.rpl_dbus_config values ('databus2.mysql.connect_retry_wait','30') on duplicate key update value='30';" ; then
    echo -n     
  else
     echo "$0: configuring connect_retry_wait failed."
  fi
  if mysql --defaults-file=${MYSQL_SLAVE_HOME_DIR}/my.cnf -uroot -e "set global rpl_dbus_request_config_reload=1;" ; then
    echo -n     
  else
     echo "$0: flushing of the configuration failed."
  fi
  
  echo "######## Restarting rpl_dbus instance $i #########"
  
done


