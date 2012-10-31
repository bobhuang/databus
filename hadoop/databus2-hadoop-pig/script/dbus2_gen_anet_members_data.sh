#!/bin/bash

script_dir=`dirname $0`
source ${script_dir}/setup.inc
source ${script_dir}/hadoop-setup.inc

source=$1
if [ -z "$source" ] ; then
  source=`last_dir /data/databases/ANET/ANETS/`
fi

anets_indir=/data/databases/ANET/ANETS/$source
settings_indir=/data/databases/ANET/MEMBER_ANET_SETTINGS/$source
members_indir=/data/databases/ANET/ANET_MEMBERS/$source
outdir=/user/$USER/anet_members/intermediate/$source
finaldir=/user/$USER/anet_members/final/$source

${script_dir}/run_pig.sh -f pig/seeding/anet_members.pig -param anets_indir=${anets_indir} -param settings_indir=${settings_indir} -param members_indir=${members_indir} -param outdir=$outdir -l ${logs_dir}

hadoop jar ${lib_dir}/databus2-hadoop-impl*.jar com.linkedin.databus2.hadoop.seeding.anetmembers.AnetMembersSchemaConverterMain $outdir $finaldir
