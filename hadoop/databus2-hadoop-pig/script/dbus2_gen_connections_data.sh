#!/bin/bash

script_dir=`dirname $0`
source ${script_dir}/setup.inc
source ${script_dir}/hadoop-setup.inc

source=$1
if [ -z "$source" ] ; then
  source=`last_dir /data/databases/CONNS/CONNECTIONS`
fi

if [ -z "$SCN" ] ; then
  echo "SCN value expected"
fi

indir=/data/databases/CONNS/CONNECTIONS/$source
outdir=/user/$USER/connections/$source
finaldir=/user/$USER/connections/final/$source

echo indir=$indir
echo ourdir=$outdir
echo finaldir=$finaldir

${script_dir}/run_pig.sh -f pig/seeding/connections.pig -param indir=$indir -param outdir=$outdir -l ${logs_dir}

hadoop jar lib/databus2-hadoop-impl-*.jar com.linkedin.databus2.hadoop.seeding.conns.ConnectionsDBRowCreatorMapReduce -D seq=$SCN $outdir $finaldir