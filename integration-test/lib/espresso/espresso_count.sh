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
##***********************************************************************************

# for linux JAVA_HOME will be different. This is for Mac
export JAVA_HOME=/System/Library/Frameworks/JavaVM.framework/Versions/1.6/Home
export JVM_ARGS="-Xms16384m -Xmx16384m -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseConcMarkSweepGC -XX:+HeapDumpOnOutOfMemoryError -Xloggc:${ESPRESSO_HOME}/gc.log"

cd ${ESPRESSO_HOME}
count_total=0
if [ $1 == "count" ] ; then  

    echo "Recreating user ${DB_USER}..."
   DBS=$(${MYSQL_HOME}/bin/mysql -ss -uroot -e "select schema_name from information_schema.schemata where schema_name like '${DB_NAME_ESP}%';" | awk '{print $1}')
   # ${MYSQL_HOME}/bin/mysql -ss -uroot -e "select concat('DROP DATABASE ', schema_name, ';') from information_schema.schemata where schema_name like '${DB_NAME_TEST}%';" | ${MYSQL_HOME}/bin/mysql -uroot
    echo "Reinitializing Espresso Tables..may take couple of minutes" 
    for d in $DBS
    do
        TABLES=$(${MYSQL_HOME}/bin/mysql -ss -uroot $d -e "show tables;" | awk '{print $1}')
        count=$(${MYSQL_HOME}/bin/mysql -ss -uroot $d -e "SELECT COUNT(*) from IdNamePair;")
        let "count_total +=$count"
        echo $count
    done
    echo $count_total
    sleep 20 
    echo "Done!"

fi

