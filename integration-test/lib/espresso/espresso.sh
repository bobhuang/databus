#!/bin/bash
# define espresso home
SCRIPT=`readlink -f $0`
# Absolute path this script is in, thus /home/user/bin
SCRIPTPATH=`dirname $SCRIPT`
cur_dir=$SCRIPTPATH
export ESPRESSO_HOME=${cur_dir}
export ESPRESSO_SCHEMA=${cur_dir}/../../../schemas_registry/espresso_test
export LUCENE_INDEX_ROOT=${ESPRESSO_HOME}/lucene
export LUCENE_INDEX_ROOT2=${ESPRESSO_HOME}/lucene2
# change the following if necessary
export DB_USER="espresso"
export DB_PASSWD="espresso"
export MYSQL_HOME=/usr
export DB_NAME_ESP=es_EspressoDB
export DB_NAME_TEST=es_Test

##*********************CHANGE THIS FOR YOUR MACHINE********************************** 
export REPL_DBUS_HOME=/export/apps/mysql-rpl-dbus/
export REPL_DBUS_HOME2=/export/apps/mysql-rpl-dbus2/
export REPL_MASTER2=/export/apps/mysql-master2/
##***********************************************************************************

# for linux JAVA_HOME will be different. This is for Mac
export JAVA_HOME=/export/apps/jdk/JDK-1_6_0_21
export JVM_ARGS="-Xms2g -Xmx2g -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseConcMarkSweepGC -XX:+HeapDumpOnOutOfMemoryError -Xloggc:${ESPRESSO_HOME}/gc.log"

cd ${ESPRESSO_HOME}
if [ ! -d "dist" ]; then
    echo "setup espresso single node.."
    ./espresso_setup.sh
    chmod -R 755 dist
    echo "Espresso setup complete"
fi

if [ $1 == "reset" ] ; then  

    echo "Recreating user ${DB_USER}..."
    ${MYSQL_HOME}/bin/mysql -uroot -e "grant all on *.* to ${DB_USER}@localhost identified by '${DB_USER}';"
   DBS=$(${MYSQL_HOME}/bin/mysql -ss -uroot -e "select schema_name from information_schema.schemata where schema_name like '${DB_NAME_ESP}%';" | awk '{print $1}')
   # ${MYSQL_HOME}/bin/mysql -ss -uroot -e "select concat('DROP DATABASE ', schema_name, ';') from information_schema.schemata where schema_name like '${DB_NAME_TEST}%';" | ${MYSQL_HOME}/bin/mysql -uroot
    echo "Reinitializing Espresso Tables..may take couple of minutes" 
    for d in $DBS
    do
        #TABLES=$(${MYSQL_HOME}/bin/mysql -ss -uroot $d -e "show tables;" | awk '{print $1}')
        TABLES=( "IdNamePair" )
        for t in $TABLES
        do
            ${MYSQL_HOME}/bin/mysql -ss -uroot $d -e "SET sql_log_bin = 0;truncate table $t;"
        done
    done
    sleep 40 
    echo "Truncating Tables..done!"
    echo "Stop Slave"
    ${MYSQL_HOME}/bin/mysql --defaults-file=${REPL_DBUS_HOME}/my.cnf -uroot -e "set global li_rpl_slave_name=\"\";"
    ${MYSQL_HOME}/bin/mysql --defaults-file=${REPL_DBUS_HOME}/my.cnf -uroot -e "stop slave;"
    echo "Reset Master & Slave"
    ${MYSQL_HOME}/bin/mysql -uroot -e "reset master;"
    ${MYSQL_HOME}/bin/mysql --defaults-file=${REPL_DBUS_HOME}/my.cnf -uroot -e "reset slave;"
    
    echo "Reset Binlog offsets"
    for d in $DBS
    do
        ${MYSQL_HOME}/bin/mysql -ss -uroot $d -e "select set_binlog_gen_id('$d',1);select set_binlog_seq_id('$d',1);"
    done

    echo "Start Slave"
    ${MYSQL_HOME}/bin/mysql --defaults-file=${REPL_DBUS_HOME}/my.cnf -uroot -e "start slave;"

    echo "removing lucene indexes used before"
    rm -rf ${LUCENE_INDEX_ROOT}
    rm -rf ${LUCENE_INDEX_ROOT2}
    echo "reset espresso"
fi

if [ $1 == "stop" ] ; then  
echo "Stopping espresso single node.."
jps | grep StorageNode | cut -d ' ' -f 1 | xargs kill -9
echo "stopped espresso"
fi 
 
if [ $1 == "start" ] ; then  
echo "Starting espresso single node.."
cd ${ESPRESSO_HOME}/dist/bin
#./run-storage-node.sh -c "espresso.singleNode.schemaRegistry.rootSchemaNamespace=${ESPRESSO_SCHEMA};espresso.singleNode.clusterManagerAdmin.configFile=${ESPRESSO_HOME}/dist/espresso-storage-node-pkg/conf/clusterConfig.json;espresso.singleNode.localIndexedStore.searchIndex.indexRootPath=${LUCENE_INDEX_ROOT}" -l ${ESPRESSO_HOME}/dist/espresso-storage-node-pkg/conf/storage-node-log4j-info.properties >& ${ESPRESSO_HOME}/espresso.log &
./run-storage-node.sh -c "espresso.singleNode.schemaRegistry.rootSchemaNamespace=${ESPRESSO_SCHEMA};espresso.singleNode.localIndexedStore.searchIndex.indexRootPath=${LUCENE_INDEX_ROOT}" -l ${ESPRESSO_HOME}/dist/espresso-storage-node-pkg/conf/storage-node-log4j-info.properties >& ${ESPRESSO_HOME}/espresso.log &
echo "started espresso"
fi

if [ $1 == "start_cluster_config" ] ; then
echo "Starting espresso single node.."
./run-storage-node-params.sh -c "espresso.singleNode.localIndexedStore.searchIndex.indexRootPath=${LUCENE_INDEX_ROOT}" -l ${ESPRESSO_HOME}/dist/conf/storage-node-log4j-info.properties -p ${ESPRESSO_HOME}/storage-node-cm-enable-1.properties >& ${ESPRESSO_HOME}/espresso.log &
./run-storage-node-params.sh -c "espresso.singleNode.localIndexedStore.searchIndex.indexRootPath=${LUCENE_INDEX_ROOT2}" -l ${ESPRESSO_HOME}/dist/conf/storage-node-log4j-info.properties -p ${ESPRESSO_HOME}/storage-node-cm-enable-2.properties >& ${ESPRESSO_HOME}/espresso2.log &
echo "started espresso"
fi

if [ $1 == "start_cluster_single_node" ] ; then
echo "Starting espresso single node.."
./run-storage-node-params.sh -c "espresso.singleNode.localIndexedStore.searchIndex.indexRootPath=${LUCENE_INDEX_ROOT}" -l ${ESPRESSO_HOME}/dist/conf/storage-node-log4j-info.properties -p ${ESPRESSO_HOME}/storage-node-cm-enable-${2}.properties >& ${ESPRESSO_HOME}/espresso.log &
echo "started espresso"
fi

if [ $1 == "truncate_all" ] ; then
  echo ""
fi

if [ $1 == "reset_cluster_all" ] ; then
    master_instances=("default" "/export/apps/mysql-master2/my.cnf")
    rpl_dbus_list=("${REPL_DBUS_HOME}my.cnf" "${REPL_DBUS_HOME2}my.cnf")

    for m in ${master_instances[@]}
    do
       echo "Recreating user ${DB_USER} for database instance $m ..."
       if [ $m == default ] ; then
           ${MYSQL_HOME}/bin/mysql -uroot -e "grant all on *.* to ${DB_USER}@localhost identified by '${DB_USER}';"
       else
           ${MYSQL_HOME}/bin/mysql --defaults-file=$m -uroot -e "grant all on *.* to ${DB_USER}@localhost identified by '${DB_USER}';"
       fi

       #truncate the tables
       echo "Reinitializing Espresso Tables..may take couple of minutes" 
       if [ $m == default ] ; then
           DBS=$(${MYSQL_HOME}/bin/mysql -ss -uroot -e "select schema_name from information_schema.schemata where schema_name like '${DB_NAME_ESP}%';" | awk '{print $1}')
       else
           DBS=$(${MYSQL_HOME}/bin/mysql --defaults-file=$m -ss -uroot -e "select schema_name from information_schema.schemata where schema_name like '${DB_NAME_ESP}%';" | awk '{print $1}')
       fi
 
       for d in $DBS
       do
           TABLES=( "IdNamePair" )
           for t in $TABLES
           do
               if [ $m == default ] ; then
                   ${MYSQL_HOME}/bin/mysql -ss -uroot $d -e "SET sql_log_bin = 0;truncate table $t;"
               else
                   ${MYSQL_HOME}/bin/mysql --defaults-file=$m -ss -uroot $d -e "SET sql_log_bin = 0;truncate table $t;" 
               fi
           done
           TABLESDROP=( "TestNewTable" )
           for td in $TABLESDROP
           do
               if [ $m == default ] ; then
                   ${MYSQL_HOME}/bin/mysql -ss -uroot $d -e "DROP TABLE IF EXISTS $td;"
               else
                   ${MYSQL_HOME}/bin/mysql --defaults-file=$m -ss -uroot $d -e "DROP TABLE IF EXISTS $td;" 
               fi
           done
       done
    done
    sleep 15
    echo "Truncating Tables..done!"

    for dbus_cfg in ${rpl_dbus_list[@]}
    do
       rpl_thread_list=()
       for t in `${MYSQL_HOME}/bin/mysql --defaults-file=${dbus_cfg} -uroot -e "show slave status \G;" | grep "Li_rpl_slave_name:" | awk '{print $2}'`
       do
          rpl_thread_list=("${rpl_thread_list[@]}" "\"$t\"")
       done
          rpl_thread_list=("${rpl_thread_list[@]}" "\"\"")
         
       for t in ${rpl_thread_list[@]}
       do
           ${MYSQL_HOME}/bin/mysql --defaults-file=$dbus_cfg -uroot -e "set global li_rpl_slave_name=$t;"
           
           # Stop Slave
           ${MYSQL_HOME}/bin/mysql --defaults-file=$dbus_cfg -uroot -e "stop slave;"
       done

        DBS2=$(${MYSQL_HOME}/bin/mysql --defaults-file=$dbus_cfg -ss -uroot -e "select schema_name from information_schema.schemata where schema_name like '${DB_NAME_ESP}%';" | awk '{print $1}')
        for d in $DBS2
        do
           TABLESDROP=( "TestNewTable" )
           for td in $TABLESDROP
           do
               ${MYSQL_HOME}/bin/mysql --defaults-file=${dbus_cfg} -ss -uroot $d -e "DROP TABLE IF EXISTS $td;" 
           done
        done
    done
           
       

    for m in ${master_instances[@]}
    do
        #reset master
        if [ $m == default ] ; then
          ${MYSQL_HOME}/bin/mysql -uroot -e "reset master;"
        else
          ${MYSQL_HOME}/bin/mysql --defaults-file=$m -uroot -e "reset master;"
        fi
    done

    for dbus_cfg in ${rpl_dbus_list[@]}
    do
       rpl_thread_list=()
       for t in `${MYSQL_HOME}/bin/mysql --defaults-file=${dbus_cfg} -uroot -e "show slave status \G;" | grep "Li_rpl_slave_name:" | awk '{print $2}'`
       do
          rpl_thread_list=("${rpl_thread_list[@]}" "\"$t\"")
       done
          rpl_thread_list=("${rpl_thread_list[@]}" "\"\"")

       for t in ${rpl_thread_list[@]}
       do
           # Reset Slave
           ${MYSQL_HOME}/bin/mysql --defaults-file=$dbus_cfg -uroot -e "set global li_rpl_slave_name=$t;"
	   ${MYSQL_HOME}/bin/mysql --defaults-file=$dbus_cfg -uroot -e "reset slave;"
       done
    done

    for m in ${master_instances[@]}
    do
        if [ $m == default ] ; then
           DBS=$(${MYSQL_HOME}/bin/mysql -ss -uroot -e "select schema_name from information_schema.schemata where schema_name like '${DB_NAME_ESP}%';" | awk '{print $1}')
        else
           DBS=$(${MYSQL_HOME}/bin/mysql --defaults-file=$m -ss -uroot -e "select schema_name from information_schema.schemata where schema_name like '${DB_NAME_ESP}%';" | awk '{print $1}')
        fi
        echo "Reset Binlog offsets"
        for d in $DBS
        do
           if [ $m == default ] ; then 
              ${MYSQL_HOME}/bin/mysql -ss -uroot $d -e "select set_binlog_gen_id('$d',1);select set_binlog_seq_id('$d',1);"
           else
              ${MYSQL_HOME}/bin/mysql --defaults-file=$m -ss -uroot $d -e "select set_binlog_gen_id('$d',1);select set_binlog_seq_id('$d',1);"
           fi
        done
   done

    for dbus_cfg in ${rpl_dbus_list[@]}
    do
       rpl_thread_list=()
       for t in `${MYSQL_HOME}/bin/mysql --defaults-file=${dbus_cfg} -uroot -e "show slave status \G;" | grep "Li_rpl_slave_name:" | awk '{print $2}'`
       do
          rpl_thread_list=("${rpl_thread_list[@]}" "\"$t\"")
       done
          rpl_thread_list=("${rpl_thread_list[@]}" "\"\"")

       for t in ${rpl_thread_list[@]}
       do
           # Start Slave
           ${MYSQL_HOME}/bin/mysql --defaults-file=$dbus_cfg -uroot -e "set global li_rpl_slave_name=$t;"
           ${MYSQL_HOME}/bin/mysql --defaults-file=$dbus_cfg -uroot -e "start slave;"
       done
    done

    echo "removing lucene indexes used before"
    rm -rf ${LUCENE_INDEX_ROOT}
    echo "reset espresso"
fi

if [ $1 == "cluster_increment_genid" ] ; then
    echo "Incrementing genids for all partitions"
    master_instances=("default" "/export/apps/mysql-master2/my.cnf")
    for m in ${master_instances[@]}
    do
        if [ $m == default ] ; then
           DBS=$(${MYSQL_HOME}/bin/mysql -ss -uroot -e "select schema_name from information_schema.schemata where schema_name like '${DB_NAME_ESP}%';" | awk '{print $1}')
        else
           DBS=$(${MYSQL_HOME}/bin/mysql --defaults-file=$m -ss -uroot -e "select schema_name from information_schema.schemata where schema_name like '${DB_NAME_ESP}%';" | awk '{print $1}')
        fi
        for d in $DBS
        do
           if [ $m == default ] ; then 
              ${MYSQL_HOME}/bin/mysql -ss -uroot $d -e "select set_binlog_gen_id('$d',2);select set_binlog_seq_id('$d',1);"
           else
              ${MYSQL_HOME}/bin/mysql --defaults-file=$m -ss -uroot $d -e "select set_binlog_gen_id('$d',2);select set_binlog_seq_id('$d',1);"
           fi
        done
    done
    echo "GenId Incremented"
fi

if [ $1 == "reset_cluster" ] ; then

    echo "Recreating user ${DB_USER}..."
    if [ $2 == default ] ; then
        ${MYSQL_HOME}/bin/mysql -uroot -e "grant all on *.* to ${DB_USER}@localhost identified by '${DB_USER}';"
    else
        ${MYSQL_HOME}/bin/mysql --defaults-file=$2 -uroot -e "grant all on *.* to ${DB_USER}@localhost identified by '${DB_USER}';"
    fi

    #truncate the tables
    echo "Reinitializing Espresso Tables..may take couple of minutes" 
    if [ $2 == default ] ; then
        DBS=$(${MYSQL_HOME}/bin/mysql -ss -uroot -e "select schema_name from information_schema.schemata where schema_name like '${DB_NAME_ESP}%';" | awk '{print $1}')
    else
        DBS=$(${MYSQL_HOME}/bin/mysql --defaults-file=$2 -ss -uroot -e "select schema_name from information_schema.schemata where schema_name like '${DB_NAME_ESP}%';" | awk '{print $1}')
    fi 
    for d in $DBS
    do
        #if [ $2 == default ] ; then
        #    TABLES=$(${MYSQL_HOME}/bin/mysql -ss -uroot $d -e "show tables;" | awk '{print $1}')
        #else
        #    TABLES=$(${MYSQL_HOME}/bin/mysql --defaults-file=$2 -ss -uroot $d -e "show tables;" | awk '{print $1}')
        #fi
        TABLES=( "IdNamePair" )
        for t in $TABLES
        do
            if [ $2 == default ] ; then
                ${MYSQL_HOME}/bin/mysql -ss -uroot $d -e "SET sql_log_bin = 0;truncate table $t;"
            else
                ${MYSQL_HOME}/bin/mysql --defaults-file=$2 -ss -uroot $d -e "SET sql_log_bin = 0;truncate table $t;"
            fi
        done
    done
    #do it for the second master
    sleep 40 
    echo "Truncating Tables..done!"
    echo "Stop Slave"
    if [ $3 == default ]; then
        ${MYSQL_HOME}/bin/mysql --defaults-file=${REPL_DBUS_HOME}/my.cnf -uroot -e "set global li_rpl_slave_name=\"\";"
        ${MYSQL_HOME}/bin/mysql --defaults-file=${REPL_DBUS_HOME2}/my.cnf -uroot -e "set global li_rpl_slave_name=\"\";"
    else
        ${MYSQL_HOME}/bin/mysql --defaults-file=${REPL_DBUS_HOME}/my.cnf -uroot -e "set global li_rpl_slave_name=$3;"
        ${MYSQL_HOME}/bin/mysql --defaults-file=${REPL_DBUS_HOME2}/my.cnf -uroot -e "set global li_rpl_slave_name=$3;"
    fi
    ${MYSQL_HOME}/bin/mysql --defaults-file=${REPL_DBUS_HOME}/my.cnf -uroot -e "stop slave;"
    ${MYSQL_HOME}/bin/mysql --defaults-file=${REPL_DBUS_HOME2}/my.cnf -uroot -e "stop slave;"
    echo "Reset Master & Slave"
    if [ $2 == default ] ; then
        ${MYSQL_HOME}/bin/mysql -uroot -e "reset master;"
    else
        ${MYSQL_HOME}/bin/mysql --defaults-file=$2 -uroot -e "reset master;"
    fi
    ${MYSQL_HOME}/bin/mysql --defaults-file=${REPL_DBUS_HOME}/my.cnf -uroot -e "reset slave;"
    ${MYSQL_HOME}/bin/mysql --defaults-file=${REPL_DBUS_HOME2}/my.cnf -uroot -e "reset slave;"
    
    echo "Reset Binlog offsets"
    for d in $DBS
    do
        if [ $2 == default ] ; then 
             ${MYSQL_HOME}/bin/mysql -ss -uroot $d -e "select set_binlog_gen_id('$d',1);select set_binlog_seq_id('$d',1);"
        else
             ${MYSQL_HOME}/bin/mysql --defaults-file=$2 -ss -uroot $d -e "select set_binlog_gen_id('$d',1);select set_binlog_seq_id('$d',1);"
        fi
    done

    echo "Start Slave"
    ${MYSQL_HOME}/bin/mysql --defaults-file=${REPL_DBUS_HOME}/my.cnf -uroot -e "start slave;"
    ${MYSQL_HOME}/bin/mysql --defaults-file=${REPL_DBUS_HOME2}/my.cnf -uroot -e "start slave;"

    echo "removing lucene indexes used before"
    rm -rf ${LUCENE_INDEX_ROOT}
    echo "reset espresso"
fi

if [ $1 == "setup" ] ; then  
echo "setup espresso single node.."
cd ${ESPRESSO_HOME}
./espresso_setup.sh
chmod -R 755 dist
echo "Espresso setup complete"
fi 
