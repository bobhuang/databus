#!/bin/bash

script_dir=`dirname $0`
source ${script_dir}/setup.inc
source ${script_dir}/hadoop-setup.inc

source=$1
if [ -z "$source" ] ; then
  source=`last_dir /data/databases/ANET/ANETS/`
fi
echo "source dir=$source"


anets_indir=/data/databases/ANET/ANETS/$source
settings_indir=/data/databases/ANET/MEMBER_ANET_SETTINGS/$source
outdir=/user/$USER/anets/intermediate/$source
finaldir=/user/$USER/anets/final/$source



${script_dir}/run_pig.sh -f pig/seeding/anet.pig -param anets_infile=${anets_indir} -param settings_infile=${settings_indir} -param outfile=$outdir -l ${logs_dir}

hadoop jar ${lib_dir}/databus2-hadoop-impl*.jar com.linkedin.databus2.hadoop.seeding.anets.AnetsSchemaConverterMain  $outdir $finaldir
