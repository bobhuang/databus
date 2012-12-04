#!/bin/bash
. /etc/profile

HOST=$1
shift 1
if [[ -z $HOST ]]; then
	exit 1
fi

PORT=12930
USER=espresso
PASSWORD=espresso

function log {
    echo -n "`date +'[%F %T] '`"
    echo "$@"
}

function log_lines {
        while read line; do
            log "$*" "$line"
        done  
}

function execmysql {
	local HOSTS=( $1 )
	if [[ ${#HOSTS[@]} -eq 1 ]]; then
		mysql -h $1 -u $USER -p$PASSWORD --port $PORT --batch -e "$2"
	else
		false; while [[ $? != 0 ]]; do
			mcmd -t 600 -n $(echo $1 | tr ' ' ',') -- mysql -h MCMD -u $USER -p$PASSWORD --port $PORT --batch -e "'$2'"
		done
	fi
}

function parseexec {
	( local toparse=`cat`
	local HOSTNAMES=`echo "$toparse" | tail -n2 | awk -F: '{print $2}' | tr ',' ' '`
	doprint=0
	echo "$toparse" | while read line; do
		if [[ `echo "$line" | grep -E '^(Success|Failure)' | wc -l` -gt 0 ]]; then
			return
		fi
		for dh in $HOSTNAMES; do
			if [[ `echo "$line" | grep -E "^$dh" | wc -l` -gt 0 ]]; then
				if [[ "$1" == "$dh" ]]; then
					doprint=1
					line=`echo "$line" | awk -F"$1:" '{print $2}'`
					break
				else
					doprint=0
					break
				fi
			fi
		done
		if [[ $doprint -eq 1 ]]; then
			echo "$line"
		fi
	done ) | head -n-1		
}

function get_named_column {
	local i=0
	local columns=''

	read columns

	for colname in $columns; do
		((i++))
		if [[ "$1" == "$colname" ]]; then
			local colnum=$i
			break
		fi
	done

	if [[ -z $colnum ]]; then
		exit 1
	fi

	while read line; do
		if [[ -z $2 || $(echo -e "$columns\n$line" | get_named_column $2) == "$3" ]]; then
			echo "$line" | awk -F"\t" "{print \$$colnum}"
		fi
	done
}

function get_slaves {
	execmysql "$1" 'show processlist;' | get_named_column Host User rplespresso | awk -F: '{print $1}' | sort -u
}

function get_espresso_dbs {
	execmysql "$1" 'show databases like "es_%";' | tail -n+2
}

function get_tables {
	execmysql "$1" "use \`$2\`; show table status;" | get_named_column Name
}

function get_maxid {
	execmysql "$1" "select max(\`$4\`) from \`$2\`.\`$3\`;" | tail -n1
}

function get_lowest_maxid {
	local L=0
	for slave in $1; do
		local this=$(get_maxid $slave $2 $3 $4)	
		if [[ $L -eq 0 || $this -lt $L ]]; then
			L=$this
		fi
	done
	echo $L
}

function maxid_where {
#	if [[ $1 -gt 0 ]]; then
#		echo -n " where \`$2\` < $1 "
#	fi
	echo -n ""
}

function get_column_name {
	execmysql "$1" "describe \`$2\`.\`$3\`;" | tail -n+2 | awk -F"\t" "NR==$4 {print \$1}" 
}

function select_named_column {
	execmysql "$1" "select \`$4\` from \`$2\`.\`$3\` $(maxid_where $5 $6);" | tail -n+2
}

function count_rows {
	execmysql "$1" "select count(1) from \`$2\`.\`$3\` $(maxid_where $4 $5);" 
}

log "$HOST: Checking row counts"

SLAVES=$*
if [[ -z "$SLAVES" ]]; then
	SLAVES=$(get_slaves $HOST)
	if [[ -z "$SLAVES" ]]; then
		log "$HOST: No slaves are currently online, please specify hostnames to use as slaves."
		exit 1
	fi
	log "$HOST: Got slaves" $SLAVES
else
	log "$HOST: Checking against specified slaves $SLAVES"
fi

for db in `get_espresso_dbs $HOST`; do
	for table in `get_tables $HOST $db`; do
		fcolname=$(get_column_name $HOST $db $table 1)
		maxid=$(get_maxid "$SLAVES" $db $table $fcolname)
		OK=1
		allrows=''
		counts=$(count_rows "$HOST $SLAVES" $db $table $fcolname $maxid)
		for checkhost in $HOST $SLAVES; do
			rows=$(echo "$counts" | parseexec $checkhost | tail -n1)
			if [[ "$checkhost" == "$HOST" ]]; then
				HOSTROWS=$rows
			fi
			allrows="$allrows $rows"
			log "$checkhost:$db.$table$text: Rowcount $rows"
			if [[ $HOSTROWS -ne $rows ]]; then
				OK=0
				ids=$(select_named_column "$HOST $checkhost" $db $table $fcolname $fcolname $maxid)
				diff -u <(select_named_column "$HOST" $db $table $fcolname $fcolname $maxid) <(select_named_column "$checkhost" $db $table $fcolname $fcolname $maxid) | grep -E '^[-+][^-+]' | log_lines "$checkhost:$db.$table($fcolname): Rowdiff "
			fi
		done
		if [[ $OK -eq 1 ]]; then
			log `echo $HOST $SLAVES | tr ' ' '/'`":$db.$table: Result OK" $allrows
		else
			log `echo $HOST $SLAVES | tr ' ' '/'`":$db.$table: Result FAIL" $allrows
		fi
	done
done



