#!/bin/bash
# define espresso home
SCRIPT=`readlink -f $0`
# Absolute path this script is in, thus /home/user/bin
SCRIPTPATH=`dirname $SCRIPT`
cur_dir=$SCRIPTPATH
export ESPRESSO_HOME=${cur_dir}
export ESPRESSO_SCHEMA=${cur_dir}/../../../schemas_registry/espresso_test
export LUCENE_INDEX_ROOT=${ESPRESSO_HOME}/lucene
# change the following if necessary
export DB_USER="espresso"
export DB_PASSWD="espresso"
export MYSQL_HOME=/usr
export DB_NAME_ESP=es_EspressoDB
export DB_NAME_TEST=es_Test

##*********************CHANGE THIS FOR YOUR MACHINE********************************** 
export REPL_DBUS_HOME=/export/apps/mysql-rpl-dbus/
export REPL_DBUS_HOME2=/export/apps/mysql-rpl-dbus2/
##***********************************************************************************

# for linux JAVA_HOME will be different. This is for Mac
export JAVA_HOME=/export/apps/jdk/JDK-1_6_0_21
export JVM_ARGS="-Xms2g -Xmx2g -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseConcMarkSweepGC -XX:+HeapDumpOnOutOfMemoryError -Xloggc:${ESPRESSO_HOME}/gc.log"
echo $1 $2 $3
cd ${ESPRESSO_HOME}
if [ ! -d "dist" ]; then
    echo "setup espresso single node.."
    ./espresso_setup.sh
    chmod -R 755 dist
    echo "Espresso setup complete"
fi


if [ $1 == "createrplthread" ] ; then
    ${MYSQL_HOME}/bin/mysql --defaults-file=${REPL_DBUS_HOME}/my.cnf -uroot -v -e "set global li_rpl_slave_name=$3;"
    ${MYSQL_HOME}/bin/mysql --defaults-file=${REPL_DBUS_HOME}/my.cnf -uroot -v -e "stop slave;"
    ${MYSQL_HOME}/bin/mysql --defaults-file=${REPL_DBUS_HOME}/my.cnf -uroot -v -e "change master to master_host='$2',master_user='rplespresso', master_password='espresso';"

fi


if [ $1 == "reset" ] ; then  

    echo "Recreating user ${DB_USER}..."
    if [ $2 == "localhost" ] ; then
      ${MYSQL_HOME}/bin/mysql -uroot -e "grant all on *.* to ${DB_USER}@localhost identified by '${DB_USER}';"
      DBS=$(${MYSQL_HOME}/bin/mysql -ss -uroot -e "select schema_name from information_schema.schemata where schema_name like '${DB_NAME_ESP}%';" | awk '{print $1}')
    else
      DBS=$(${MYSQL_HOME}/bin/mysql -ss -urplespresso -pespresso -h$2 -e "select schema_name from information_schema.schemata where schema_name like '${DB_NAME_ESP}%';" | awk '{print $1}')
    fi 
    echo "Reinitializing Espresso Tables..may take couple of minutes" 
    for d in $DBS
    do
        if [ $2 == "localhost" ] ; then
            ${MYSQL_HOME}/bin/mysql -ss -uroot $d -e "truncate table IdNamePair;"
        else
            ${MYSQL_HOME}/bin/mysql -ss -urplespresso -pespresso -h$2 $d -e "truncate table IdNamePair;"
        fi
    done
    sleep 20 
    echo "Truncating Tables..done!"
    echo "Stop Slave"
    if [ $3 == "default" ] ; then 
        ${MYSQL_HOME}/bin/mysql --defaults-file=${REPL_DBUS_HOME}/my.cnf -uroot -e "set global li_rpl_slave_name=\"\";"
        ${MYSQL_HOME}/bin/mysql --defaults-file=${REPL_DBUS_HOME2}/my.cnf -uroot -e "set global li_rpl_slave_name=\"\";"
    else
        ${MYSQL_HOME}/bin/mysql --defaults-file=${REPL_DBUS_HOME}/my.cnf -uroot -e "set global li_rpl_slave_name=$3;"
        ${MYSQL_HOME}/bin/mysql --defaults-file=${REPL_DBUS_HOME2}/my.cnf -uroot -e "set global li_rpl_slave_name=$3;"
    fi

    ${MYSQL_HOME}/bin/mysql --defaults-file=${REPL_DBUS_HOME}/my.cnf -uroot -e "stop slave;"
    ${MYSQL_HOME}/bin/mysql --defaults-file=${REPL_DBUS_HOME2}/my.cnf -uroot -e "stop slave;"
    echo "Reset Master & Slave"
    if [ $2 == "localhost" ] ; then
        ${MYSQL_HOME}/bin/mysql -uroot -e "reset master;"
    else
        ${MYSQL_HOME}/bin/mysql -urplespresso -pespresso -h$2 -e "reset master;"
    fi 

    ${MYSQL_HOME}/bin/mysql --defaults-file=${REPL_DBUS_HOME}/my.cnf -uroot -e "reset slave;"
    ${MYSQL_HOME}/bin/mysql --defaults-file=${REPL_DBUS_HOME2}/my.cnf -uroot -e "reset slave;"
    
    echo "Reset Binlog offsets"
    for d in $DBS
    do
        if [ $2 == "localhost" ] ; then
          ${MYSQL_HOME}/bin/mysql -ss -uroot $d -e "select set_binlog_gen_id('$d',1);select set_binlog_seq_id('$d',1);"
        else
          ${MYSQL_HOME}/bin/mysql -ss -urplespresso -pespresso -h$2 $d -e "select set_binlog_gen_id('$d',1);select set_binlog_seq_id('$d',1);"
        fi
    done

    echo "Start Slave"
    ${MYSQL_HOME}/bin/mysql --defaults-file=${REPL_DBUS_HOME}/my.cnf -uroot -e "start slave;"
    ${MYSQL_HOME}/bin/mysql --defaults-file=${REPL_DBUS_HOME2}/my.cnf -uroot -e "start slave;"

    echo "removing lucene indexes used before"
    rm -rf ${LUCENE_INDEX_ROOT}
    echo "reset espresso"
fi

if [ $1 == "stop" ] ; then  
echo "Stopping espresso single node.."
jps | grep EspressoSingleNode | cut -d ' ' -f 1 | xargs kill -9
echo "stopped espresso"
fi 
 
if [ $1 == "start" ] ; then  
echo "Starting espresso single node.."
cd ${ESPRESSO_HOME}/dist/bin

./run-storage-node.sh -c "espresso.singleNode.schemaRegistry.rootSchemaNamespace=${ESPRESSO_SCHEMA};espresso.singleNode.clusterManagerAdmin.configFile=${ESPRESSO_HOME}/dist/espresso-storage-node-pkg/conf/clusterConfig.json;espresso.singleNode.localIndexedStore.searchIndex.indexRootPath=${LUCENE_INDEX_ROOT}" -l ${ESPRESSO_HOME}/dist/espresso-storage-node-pkg/conf/storage-node-log4j-info.properties >& ${ESPRESSO_HOME}/espresso.log &
echo "started espresso"
fi

if [ $1 == "setup" ] ; then  
echo "setup espresso single node.."
cd ${ESPRESSO_HOME}
./espresso_setup.sh
chmod -R 755 dist
echo "Espresso setup complete"
fi 
