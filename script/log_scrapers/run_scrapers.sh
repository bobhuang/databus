#!/bin/bash

cd `dirname $0`
source setup.inc

mkdir -p ${data_log_dir}

script_dir=`dirname $0`

for h in $* ; do
  echo "Scraping $h ..." 
  host_file_base="${data_log_dir}/$h"
  find_errors_file="${host_file_base}-find_errors.log"
  touch ${find_errors_file}
  smart_ssh $h remote_scrapers/find_errors.py --json | egrep '^\[' >> ${find_errors_file}
done
 
