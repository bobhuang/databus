#!/bin/bash

cd `dirname $0`
source setup.inc


for h in $* ; do
  echo "Synching $h ..."
  h_uname=`smart_ssh $h uname -a`
  
  if (echo $h_uname | grep Linux) ; then
    rsync_cmd=rsync
  else
    rsync_cmd='rsync --rsync-path=/usr/local/bin/rsync'
  fi

  if (echo $h | grep '\.prod' ) ; then
    rsync -r ./remote_scrapers eng-portal.corp.linkedin.com:~ 
    ssh -ttA eng-portal.corp.linkedin.com ${rsync_cmd} -r ./remote_scrapers $h:~
  else
    ${rsync_cmd} -r ./remote_scrapers $h:~
  fi
done
