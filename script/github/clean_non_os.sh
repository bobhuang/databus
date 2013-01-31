#!/bin/bash 

script_dir=`dirname $0`
root_dir=$script_dir/../..

if [ "$1" == "--dry-run" ] ; then
	dry_run=1
fi

function cleanup(){
	echo "Removing $d ..."
	if [ "$dry_run" != "1" ] ; then
		rm -rf $d
	fi
}

declare -a non_os_dirs=( 'databus3-core' 'databus3-relay' 'databus3-client' 'databus3-scripts' 'databus3-linkedin-relay' 'perf' 'integration-test' 'sitetools' 'schemas_registry' 'databus-events' 'databus-profile-client' 'rmiservers' 'databus2-examples' 'mysql-5.5' 'hadoop' 'central-config-repo' 'sec-consumer' 'databus-profile-relay' 'test-consumer-war' 'databus-relay' 'databus2-linkedin-wars') 

declare -a non_os_files=( 'restart_glu.sh' 'settings.gradle.cc' 'svninfo' 'CHANGELOG')

# clean-up top-level directories
for d in "${non_os_dirs[@]}" ; do
	dir_path=$root_dir/$d
	cleanup $d
done

# clean up script
for f in $root_dir/script/* $root_dir/tools/* ; do
  if [ $f != $root_dir/script/setup.inc -a $f != $root_dir/script/github ] ; then
	cleanup $f
  fi
done

# other clean-up
for d in "${non_os_files[@]}" ; do
	dir_path=$root_dir/$d
	cleanup $d
done


