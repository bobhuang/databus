#!/bin/bash

script_dir=`dirname $0`
source ${script_dir}/setup.inc

function usage() {
	echo "USAGE: $0 from_tag to_tag"
	exit 1
}

declare -a tracked_files=( 'databus2-bootstrap' 'databus2-relay' 'databus-bootstrap-producer' 'databus-cluster' 'databus-util-cmdline' 'build.gradle'  'databus2-client' 'databus-bootstrap-server'  'databus-core' 'doc' 'codecoverage' 'databus2-example' 'databus-bootstrap-client'  'databus-bootstrap-utils' 'databus-group-leader' 'javadoc'  'settings.gradle' 'database' 'databus-bootstrap-common' 'databus-client' 'databus-relay' 'project-spec.json' 'defaultEnvironment.gradle' 'README' )

declare -a excludes=( 'databus-relay/databus-relay-run' )

from_tag=$1
to_tag=$2

function merge_internal() {
	if [ -z "$from_tag" -o -z "$to_tag" ] ; then
		usage
	fi

	to_branch=${to_tag}${github_suffix}
	read -p "Press ENTER to create work branch ${to_branch}"
	git branch ${to_branch} ${github_branch}
	git checkout ${to_branch}

	patch_file=${from_tag}-${to_tag}.patch
	echo "Generating patch ${patch_file} ... "
	git log -p --binary --reverse ${from_tag}..${to_tag} -- "${tracked_files[@]}" > ${patch_file}

	read -p "Press ENTER to apply ${patch_file}"
	exclude_options=
	for x in "${excludes[@]}" ; do
		exclude_options="${exclude_options} --exclude=$x/**"
	done

	echo exclude=$exclude_options
	git apply --reject ${exclude_options}  -- ${patch_file}

	echo "Checking for rejected commits"
	declare -a rejects=( $(find . -name "*.rej") )
	if [ "${#rejects[@]}" -gt 0 ] ; then
		echo "REJECTS FOUND:"
		for r in "${rejects[@]}" ; do
			echo $r
		done
		exit 1
	fi
}

#main90

merge_internal 2>&1 | tee ${from_tag}-${to_tag}.log
