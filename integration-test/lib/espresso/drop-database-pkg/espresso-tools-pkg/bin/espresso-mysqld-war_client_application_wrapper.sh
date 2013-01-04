#!/usr/bin/env bash
#
# Drop this in /etc/profile.d or source it via .bash_profile

export MYSQLD_WAR_BASEDIR="/export/content/glu/apps/espresso-mysqld"
export MYSQLD_WAR_DATADIR="/export/content/data/mysql"
export MYSQLD_WAR_INSTANCES=`ls $MYSQLD_WAR_BASEDIR`

function mysql_war_wrapper {
	local bname=$1
	shift 1
	if [[ $1 =~ i[0-9][0-9][0-9] ]]; then
		local instance=$1
		shift 1
		local binary="$MYSQLD_WAR_BASEDIR/$instance/tmp/mysql/bin/$bname"
		if [[ ! -d `dirname $binary` ]]; then
			echo "`dirname $binary` not found.  Is espresso-mysqld-war-$instance running?" >&2
			return
		elif [[ ! -f "$binary" ]]; then
			echo "$binary not found." >&2
			return
		elif [[ ! -x "$binary" ]]; then
			echo "You do not have permission to execute $binary." >&2
			return
		else
			if [[ `$binary --help | grep -- '--socket' | wc -l` -gt 0 ]]; then
				local socket="$MYSQLD_WAR_DATADIR/$instance/mysql.sock"
				if [[ ! -S "$socket" ]]; then
					echo "$socket does not exist. Is espresso-mysqld-war-$instance running?" >&2
					return
				fi
				local clientopts="--socket=$socket"
			fi
			$binary $clientopts "$@"
		fi
	else
		echo "Invalid instance specified.  Format: $bname [$(echo "$MYSQLD_WAR_INSTANCES" | tr "\n" "|")] <opts...>" >&2
		false
	fi
}

for binary_name in `ls $MYSQLD_WAR_BASEDIR/*/tmp/mysql/bin/* | xargs -n1 basename | sort -u`; do
	alias $binary_name="mysql_war_wrapper $binary_name"
done
