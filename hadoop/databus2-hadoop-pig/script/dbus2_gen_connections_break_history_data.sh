#!/bin/bash

script_dir=`dirname $0`
source ${script_dir}/setup.inc
source ${script_dir}/hadoop-setup.inc

source=$1
if [ -z "$source" ] ; then
  source=`last_dir /data/databases/CONNS/CONNECTION_BREAK_HISTORY`
fi

indir=/data/databases/CONNS/CONNECTION_BREAK_HISTORY/$source
outdir=/user/$USER/connectionsbreakhistory/$source

echo indir=$indir
echo ourdir=$outdir

${script_dir}/run_pig.sh -f pig/seeding/connections_break_history.pig -param infile=$indir -param outfile=$outdir -l ${logs_dir}
