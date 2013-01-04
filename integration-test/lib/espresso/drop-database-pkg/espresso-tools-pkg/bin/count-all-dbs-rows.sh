#/bin/bash
for database in `echo "show databases;" | mysql -h $1 -urplespresso -pespresso`; do
  if [[ $database == es_* ]] ; then
    ./count-db-rows.sh $1 $database;
  fi;
done;

if [ $? -ne 0 ]; then
  echo Usage: $0 server;
fi;

