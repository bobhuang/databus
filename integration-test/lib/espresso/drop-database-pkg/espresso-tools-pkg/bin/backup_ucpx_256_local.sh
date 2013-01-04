#/bin/bash

if [ `whoami`!=app ] ; then
  echo Need to run as app
  exit 1
fi

for i in {0..249}
do
  echo "backing up es_ucpx_$i"
  /usr/bin/xtrabackup-wrapper --snapshot-database=es_ucpx_$i --user=espresso --password=espresso --defaults-file=/etc/my.cnf --stream=tar /export/content/data/backupRestore/ | pv -q -L240m | pigz -p 12 - > /dev/null
done

