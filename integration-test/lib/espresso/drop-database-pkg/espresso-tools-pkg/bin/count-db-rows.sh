#/bin/bash
export str_exec="select '$2' as db, (select 0 ";
for table in `echo "show tables;" | mysql -h $1 -urplespresso -pespresso $2`; do
  if [ "$table" != "Tables_in_$2" ] ; then
    export str_exec="$str_exec + (select count(*) from $table)";
  fi;
done;

echo "$str_exec) as rows, '$1' as server, get_binlog_gen_id('$2') as generation, get_binlog_seq_id('$2') as sequence;" | mysql -h $1 -urplespresso -pespresso $2 | grep -v "db.*rows.*server.*"

if [ $? -ne 0 ]; then
  echo Usage: serverName dbName;
fi;

