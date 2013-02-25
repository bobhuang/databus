#!/bin/bash

script_dir=`dirname $0`

cd ${script_dir}/etools/bin

echo "##### Uploading UCPX schemas ######"
./schema-uploader.sh --schema-src-root ../../ucp512 --schema-dst-root schemas_registry/ESPRESSO_DEV_FT_3 --sync-method UPDATE --zk-address eat1-app207.stg.linkedin.com:12913 --upload-db-regex ucpx --upload-db-alias ucpx

echo "##### Uploading UCP schemas ######"
./schema-uploader.sh --schema-src-root ../../ucp512 --schema-dst-root schemas_registry/ESPRESSO_DEV_FT_3 --sync-method UPDATE --zk-address eat1-app207.stg.linkedin.com:12913 --upload-db-regex ucp --upload-db-alias ucp

echo "##### Setting up storage node cluster ######"
./setup_cluster.sh "eat1-app113.corp:12929" ESPRESSO_DEV_FT_3 "eat1-app103.corp.linkedin.com:20801 eat1-app177.corp.linkedin.com:20801 eat1-app178.corp.linkedin.com:20801 eat1-app179.corp.linkedin.com:20801 eat1-app180.corp.linkedin.com:20801 eat1-app181.corp.linkedin.com:20801" "ucp,512,3 ucpx,512,3" ../../ucp512 DevClustersV4 "eat1-app113.corp.linkedin.com:12924 eat1-app114.corp.linkedin.com:12924"

echo "##### Setting up relay cluster ######"
for rlyhost in eat1-app171.corp.linkedin.com eat1-app172.corp.linkedin.com ; do
  for rlyport in 28101 28102 28103 28104 28105 28106 28107 28108 28109 ; do
     relays="$relays $rlyhost:$rlyport"
  done
done
./add_relays_2_cm.sh eat1-app113.corp:12929 RELAY_DEV_FT_3 "$relays" DevClustersV4
