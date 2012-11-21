#!/bin/bash

script_dir=`dirname $0`
source ${script_dir}/setup.inc
source ${script_dir}/hadoop-setup.inc

source=$1
if [ -z "$source" ] ; then
  source=`last_dir /data/databases/ANET/ANETS/`
fi

emails_indir=/data/databases/EOS/EMAILS/$source
member_emails_indir=/data/databases/EOS/MEMBER_EMAILS/$source
outdir=/user/$USER/eos/$source

${script_dir}/run_pig.sh -f pig/seeding/eos.pig -param emails_indir=${emails_indir} -param member_emails_indir=${member_emails_indir} -param outdir=$outdir -l ${logs_dir}