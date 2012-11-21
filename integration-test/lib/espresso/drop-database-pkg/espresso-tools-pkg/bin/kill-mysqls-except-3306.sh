#/bin/bash
echo Killing mysql daemons
for pid in `ps aux | grep -ie "^[a-z][a-z]* [ ]*.*mysqld_safe" | grep -v root | grep -v grep | sed -e "s/^[a-z][a-z]* [ ]*\([1-9][0-9]*\) .*/\1/1"`
do
  echo killing mysqld_safe with pid $pid
  ps u --no-heading -p $pid
  kill -9 $pid
done

echo Killing mysqld processes
for pid in `ps aux | grep -ie "^[a-z][a-z]* [ ]*.*/usr/sbin/mysqld.*port" | grep -v port\=3306 | grep -v grep | sed -e "s/^[a-z][a-z]* [ ]*\([1-9][0-9]*\) .*/\1/1"`
do
  echo killing mysqld with pid $pid
  ps u --no-heading -p $pid
  kill -9 $pid
done

