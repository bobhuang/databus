#!/bin/bash

script_dir=`dirname $0`
source ${script_dir}/setup.inc
source ${script_dir}/hadoop-setup.inc

SRCID=601
SRC_TAB_NAME=tab_${SRCID}
source=$1
part_n=$2
destdir=$3

if [ -z "$source" ] ; then
  source=`last_dir /user/$USER/connections/final/`
fi

if [ -z "$destdir" ] ; then
	destdir=.
fi

finaldir=/user/$USER/connections/final/$source
echo finaldir=$finaldir

if [ -z "${part_n}" ] ; then
	part_n=`last_dir $finaldir/ | sed -e 's/[^1-9]*\([0-9][0-9]*\).*/\1/'`
	echo part_n=${last_file}
fi

if [ -z "$SCN" ] ; then
  echo "SCN expected"
  exit 1
fi

user=${BOOTSTRAP_USER:-bootstrap}
pass=${BOOTSTRAP_PASSWORD:-bootstrap}

function async_copy() {
  echo "Starting download from ${start_n} ..."
	sleep_s=$1
	for ((i=start_n; i<=part_n ;++i)) ; do
		file_name=`printf 'part-%05i.deflate' $i`
		echo "Copying ${file_name} ..."
		time bin/hadoop.sh dfs -copyToLocal $finaldir/${file_name} $destdir
		echo "Sleeping for ${sleep_s}s ..."
		sleep "${sleep_s}s"
	done
}

# main()

if (mysql -u${user} -p${pass} -e "show databases;" | grep bootstrap) ; then
  echo "Bootstrap DB found!"
else
  read -n 1 -p "Create a new bootstrap DB (Y/N)" do_create_db
  if [ "${do_create_db}" == "Y" -o "${do_create_db}" == "y" ] ; then
          bin/databus2-bootstrap/bootstrap_db_reinit.sh
  fi
fi

if (mysql -u${user} -p${pass} -Dbootstrap -e "show tables like '${SRC_TAB_NAME}';" | grep ${SRC_TAB_NAME}) ; then
  echo "Dropping ${SRC_TAB_NAME} table ... "
  mysql -u${user} -p${pass} -Dbootstrap -e "DROP TABLE ${SRC_TAB_NAME};"
fi

echo "Creating table ${SRC_TAB_NAME} ..."
bin/dbus2_bootstrapdb_ctrl.sh -a CREATE_TAB_TABLE -S ${SRCID} -c conf/sources-conn-ela4.json

echo "Removing unique srckey index from ${SRC_TAB_NAME} for better performance ..."
mysql -u${user} -p${pass} -Dbootstrap -e "alter table ${SRC_TAB_NAME} drop index srckey;"

# figure out all missing data files; remove the last data file to avoid partial data
for ((start_n=0; start_n<=part_n; ++start_n)) ; do
  file_name=`printf 'part-%05i.deflate' $start_n`
  if [ ! -f $destdir/${file_name} ] ; then
    break
  fi 
done

if [ ${start_n} -gt 0 ] ; then
  start_n=$((start_n-1))
  file_name=`printf 'part-%05i.deflate' $start_n`
  rm $destdir/${file_name}
fi

async_copy 60 &

for ((j=0; j<=part_n ;++j)) ; do
	file_name=`printf 'part-%05i' $j`
	
	curfile=$destdir/${file_name}.deflate
	
	while [ ! -f $curfile ] ; do
	   echo "Waiting for file ${file_name}.deflate ..."
	   sleep 10
	done
	
	echo "Extracting ${file_name}.deflate ..."
	time bin/dbus2_inflate.sh $curfile
	echo "Loading data in mysql"
	time mysql -u${user} -p${pass} -Dbootstrap -e "SET UNIQUE_CHECKS=0;LOAD DATA LOCAL INFILE '$destdir/${file_name}' INTO TABLE tab_601 FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' (srckey,@val1) set scn=${SCN}, val=UNHEX(@val1);"
	echo "Loading done." 
	rm $destdir/${file_name}
  rm ${curfile}
done

echo "Creating unique srckey index on ${SRC_TAB_NAME} ..."
time mysql -u${user} -p${pass} -Dbootstrap -e "CREATE UNIQUE INDEX srckey ON ${SRC_TAB_NAME} (srckey );"

echo "Setting up checkpoints"
sudo -u app bin/run-cp-serializer-bst-producer-conn.sh -a CHANGE -s ${SCN} -t ONLINE_CONSUMPTION

echo "Number of rows seeded"
time mysql -u${user} -p${pass} -Dbootstrap -e "SELECT count(*) FROM ${SRC_TAB_NAME};"
