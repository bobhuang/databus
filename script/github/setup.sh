#!/bin/bash

script_dir=`dirname $0`
source ${script_dir}/setup.inc

read -p "Press ENTER to tag initial_commit"
git tag -a initial_commit -m "Initial SVN commit" 799a790ba9f66fc35e5d54b6acfade76d0241413
read -p "Press ENTER to create initial_commit_github branch"
git branch ${github_initial_branch} ${github_initial_tag}
git checkout ${github_initial_branch}
read -p "Press ENTER to cleanup non-O/S files"
${script_dir}/clean_non_os.sh
read -p "Press ENTER to commit clean up"
git commit -am "github push ready version for initial_commit"
git tag -a ${github_initial_final} -m "github push ready version for initial_commit"
read -p "Press ENTER to create root branch"
git branch ${github_branch} ${github_initial_final}

