#!/bin/bash

cd `dirname $0`
source setup.inc

mkdir -p ${report_dir}

local_processors/find_errors_local.py -L ${data_log_dir} -F TXT 2>&1 > ${report_dir}/find_errors.txt
local_processors/find_errors_local.py -L ${data_log_dir} -F HTML 2>&1 > ${report_dir}/find_errors.html

echo "Subject: Log scrape report for ${report_date_str}" > ${report_dir}/mail.txt
echo "Content-Type: text/html" >> ${report_dir}/mail.txt
#echo "MIME-Version: 1.0" >> ${report_dir}/mail.txt
echo >> ${report_dir}/mail.txt
echo "<html>" >> ${report_dir}/mail.txt
echo "<h1>Scrape report for ${report_date_str}</h1>" >> ${report_dir}/mail.txt
cat ${report_dir}/find_errors.html >> ${report_dir}/mail.txt
echo "</html>" >> ${report_dir}/mail.txt

/usr/sbin/sendmail databus-alerts@linkedin.com < ${report_dir}/mail.txt  
