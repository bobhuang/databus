#!/bin/bash
if [ "$1" == "default" ] 
then
   echo "deleting from defaults mysql instance"
   mysql -ss -uroot -e "select concat('DROP DATABASE ', schema_name, ';')  from information_schema.schemata where schema_name like 'es_TestDB%'" | mysql -uroot
   mysql -ss -uroot -e "select concat('DROP DATABASE ', schema_name, ';')  from information_schema.schemata where schema_name like 'es_EspressoDB%'" | mysql -uroot
else
   echo "deleting from mysql instance $1"
   mysql --defaults-file=/export/apps/$1/my.cnf -ss -uroot -e "select concat('DROP DATABASE ', schema_name, ';')  from information_schema.schemata where schema_name like 'es_TestDB%'" | mysql --defaults-file=/export/apps/$1/my.cnf -ss -uroot
   mysql --defaults-file=/export/apps/$1/my.cnf -ss -uroot -e "select concat('DROP DATABASE ', schema_name, ';')  from information_schema.schemata where schema_name like 'es_EspressoDB%'" | mysql --defaults-file=/export/apps/$1/my.cnf -ss -uroot
fi

