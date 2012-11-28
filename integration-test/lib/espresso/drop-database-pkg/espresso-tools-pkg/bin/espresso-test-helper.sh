#!/bin/bash

# This script will take in a root location expecting db, table, and doc schemas for a particular db name
# It will upload all of this to espresso but prefix the db name with your chosen alias
#espresso-test-helper.sh -operation=CREATE -schema-src-root=./../../schemas_registry -upload-db=MyDB -upload-db-alias=johndoe_MyDB -espresso-address=esv4-app43.stg.linkedin.com:12918

script_dir=`dirname $0`
config_dir=$script_dir/../conf
source $script_dir/setup.inc

${JAVA_HOME}/bin/java ${JVM_ARGS} -cp ${cp} com.linkedin.espresso.schema.util.EspressoTestHelper $@
