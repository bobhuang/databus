#! /bin/bash

if [ $# -lt 2 ]; then
  echo 'Usage: <hostname> <port> <full path of unwrapped mysql if mysql wrapper is used> '
  exit
fi

hostname=$1
let port=$2
mysqlBin=mysql

if [ $# -eq 3 ]; then
  mysqlBin=$3"/mysql"
fi

username="espresso"
password="espresso"
mysqlcmd="$mysqlBin --protocol=tcp --host=$hostname --port=$port -u$username -p$password"
binlogOffStmt='set sql_log_bin=0'
binlogOnStmt='set sql_log_bin=1'

oldIndexTableSuffix="Index"
newIndexTableSuffix="_index_norpl_esp"
oldIndexSCNTableName="IndexSCN"
newIndexSCNTableName="IndexSCN_norpl_esp"
newChecksumTableName="checksum_esp"

echo "Convert table names for mysql instance using command: $mysqlcmd"

dbs="$($mysqlcmd -Bse "show databases like '"es%"'")"
for db in $dbs
do
    echo "For db:"$db

    # All the index Tables
    tables="$($mysqlcmd -Bse "use $db;show tables like '"%Index"'")"
    for table in $tables
    do
	oldTableName=$db"."$table
        newTableName=${oldTableName/${oldIndexTableSuffix}/${newIndexTableSuffix}}
        echo "    $oldTableName ==> $newTableName"
        renameStmt="rename table $oldTableName to $newTableName"
        $mysqlcmd -e "$binlogOffStmt;${renameStmt};$binlogOnStmt"
    done

    # for IndexSCN table
    tables="$($mysqlcmd -Bse "use $db;show tables like 'IndexSCN'")"
    for table in $tables
    do
	oldTableName=$db"."$table
	newTableName=$db"."${newIndexSCNTableName}
	echo "    $oldTableName ==> $newTableName"
	renameStmt="rename table $oldTableName to $newTableName"
	$mysqlcmd -e "$binlogOffStmt;${renameStmt};$binlogOnStmt"
    done    

    # for checksum table
    tables="$($mysqlcmd -Bse "use $db;show tables like 'espresso_checksum_table'")"
    for table in $tables
    do
	oldTableName=$db"."$table
	newTableName=$db"."${newChecksumTableName}
	echo "    $oldTableName ==> $newTableName"
	renameStmt="rename table $oldTableName to $newTableName"
	$mysqlcmd -e "$binlogOffStmt;${renameStmt};$binlogOnStmt"
    done
done