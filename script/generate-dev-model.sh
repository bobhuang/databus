#!/bin/bash

script_dir=`dirname $0`
root_dir=$script_dir/..
conf_dir=$root_dir/config

dest_dir=$1

if [ -z "$dest_dir" ] ; then
	dest_dir=$root_dir
fi

root_dir_fqpath=`cd $root_dir && echo $PWD`
bom_dest_file=$dest_dir/bom-mpbeta.xml
fabric_dest_file=$dest_dir/fabric-mpbeta.xml
model_dest_file=$dest_dir/databus2-glu.json

echo "Creating BOM $bom_dest_file ..."
sed -e "s|\$PROJECT_DIR|$root_dir_fqpath|g" $conf_dir/bom-mpbeta.xml.template > $bom_dest_file

echo "Generating fabric $fabric_dest_file ..."
devfabric --input $conf_dir/topology-mpbeta.xml --output $fabric_dest_file

echo "Generating model $model_dest_file ..."
gen-glu-dev --fabric $fabric_dest_file --bom $bom_dest_file --product databus2 --version dev --out $model_dest_file
