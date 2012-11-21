#!/bin/bash

script_dir=`dirname $0`
e_schedule_file="${script_dir}/espresso-schedule.txt"

function mail_reminder() {
  class=$1
  primary_today=$2
  backup_today=$3
  primary_tomorrow=$4
  backup_tomorrow=$5
  today=`date +'%Y-%m-%d'`
  
  (
   cat <<EOM
Today: ${primary_today}/${backup_today}
Tomorrow: ${primary_tomorrow}/${backup_tomorrow}

Instructions: https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Databus+On+Call+Log#DatabusOnCallLog-Instructions
EOM
) | mail -s "${class} Oncall Reminder for $today " -c espresso-ft-dev@linkedin.com ${primary_today}@linkedin.com ${backup_today}@linkedin.com ${primary_tomorrow}@linkedin.com ${backup_tomorrow}@linkedin.com
}

function process_schedule_file() {
  class=$1
  f=$2
  while read -r day primary backup ; do
    if [[ "$day" =~ '#' ]] ; then
      #ignore comments
      continue
    fi
    
    linets=`date -d "$day" +'%s'`
    bod_ts=`date -d "00:00:00" +'%s'`
    eod_ts=$((bod_ts+86399))
    eot_ts=$((eod_ts+86400))
    if [ $bod_ts -le $linets -a $eod_ts -ge $linets ] ; then
      echo "Today: " $primary $backup
      primary_today=$primary
      backup_today=$backup
    elif [ $eod_ts -lt $linets -a $eot_ts -ge $linets ] ; then
      echo "Tomorrow: " $primary $backup
      primary_tomorrow=$primary
      backup_tomorrow=$backup
    fi
  done < "$f"
  mail_reminder "$class" ${primary_today} ${backup_today} ${primary_tomorrow} ${backup_tomorrow}
}

process_schedule_file 'Espresso Databus' ${e_schedule_file}
