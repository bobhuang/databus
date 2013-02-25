#!/bin/bash

script_dir=`dirname $0`
source ${script_dir}/setup.inc

git branch -D ${github_branch}
git tag -d ${github_initial_final}
git branch -D ${github_initial_branch}
git tag -d ${github_initial_tag}
