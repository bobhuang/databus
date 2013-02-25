#!/bin/bash

echo "######## Installing glu agent ##############"
sudo yum -y install glu-agent

echo "######## Stopping mysql ##############"
sudo /etc/init.d/mysql stop

echo "######## Killing any running mysql instances ###########" 
ps -ef | grep mysqld | awk '{print $2}' | xargs sudo kill -9

echo "########## Uninstalling mysql packages #############"
rpm -qa | grep mysql | xargs -ipackage sudo rpm  -e package --nodeps

echo "######## Downloading latest mysql package #########"
sudo rpm -U http://apachev-ld.linkedin.biz/rpms/MySQL-bundle-Linkedin-5.5.8-latest.x86_64.rpm

echo "######## Setting up MySQL #########"
sudo li-setup-mysql --mode m

echo "######## Creating Lucene directories #########"
sudo mkdir /mnt/u001/lucene-indexes
sudo chown -R app:app /mnt/u001/lucene-indexes

echo "######## Granting permissions to espresso user #########"
mysql -uroot -e "grant all on *.* to espresso@'localhost' identified by 'espresso';"
mysql -uroot -e "grant all on *.* to rplespresso@'%' identified by 'espresso';"
mysql -uroot -e "grant all on *.* to espresso@'%' identified by 'espresso';"


