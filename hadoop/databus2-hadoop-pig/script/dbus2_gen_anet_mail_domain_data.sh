#!/bin/bash

script_dir=`dirname $0`
source ${script_dir}/setup.inc
source ${script_dir}/hadoop-setup.inc

source=$1
if [ -z "$source" ] ; then
  source=`last_dir /data/databases/ANET/ANETS/`
fi

indir=/data/databases/ANET/ANET_MAIL_DOMAIN/$source
outdir=/user/$USER/anet_mail_domain/final/$source


${script_dir}/run_pig.sh -f pig/seeding/anet_mail_domain.pig -param indir=${indir} -param outdir=$outdir -l ${logs_dir}
