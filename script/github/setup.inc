if [ -z "$DATABUS_GIT_ROOT" ] ; then
	DATABUS_GIT_ROOT=$script_dir/../..
fi

github_branch=github
github_suffix=_github
github_final_suffix=_github_final

github_initial_tag=initial_commit
github_initial_branch=${github_initial_tag}${github_suffix}
github_initial_final=${github_initial_tag}${github_final_suffix}
