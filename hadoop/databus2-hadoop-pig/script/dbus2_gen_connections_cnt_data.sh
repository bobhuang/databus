#!/bin/bash

script_dir=`dirname $0`
source ${script_dir}/setup.inc
source ${script_dir}/hadoop-setup.inc

source=$1
if [ -z "$source" ] ; then
  source=`last_dir /data/databases/CONNS/CONNECTIONS_CNT`
fi

indir=/data/databases/CONNS/CONNECTIONS_CNT/$source
outdir=/user/$USER/connectionscnt/$source

echo indir=$indir
echo ourdir=$outdir

${script_dir}/run_pig.sh -f pig/seeding/connections_cnt.pig -param infile=$indir -param outfile=$outdir -l ${logs_dir}
