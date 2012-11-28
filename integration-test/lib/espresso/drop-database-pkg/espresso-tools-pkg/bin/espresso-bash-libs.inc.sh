#!/bin/bash

function log {
    if is_dry_run; then
        echo -n "DRYRUN "
    fi
    echo -n "`date +'[%F %T] '`"
    echo "$@"
}

function echoexec {
    log $1
    if ! is_dry_run; then
        $1
    fi
}

function log_lines {
    local PAD=''
    while [[ ! -z "$1" ]]; do
        while read line; do
            log "$PAD$line"
        done < <(echo "$1")
        PAD="   "
        shift 1
    done
}

function do_exit {
    if [[ ! -z $PFPID ]]; then
        log "Cleaning up port forward process $PFPID"
        kill $PFPID
    fi
    
    stty echo
    log "Exited with code $1"
    exit $1
}

function control_c {
  log
  log "Caught ctrl-c, cleaning up and exiting..."
  do_exit 255
}

trap control_c SIGINT

function required {
    for varname in $@; do
        if [[ -z "${!varname}" ]]; then
            echo "LIBRARY ERROR: Variable '\$$varname' is required but is not set.  Cannot proceed, aborting."
            do_exit 100
        fi
    done
}

function cm_get {
    required CLUSTERMANAGER CLUSTERNAME
    OUTPUT=`curl http://$CLUSTERMANAGER/$1/$CLUSTERNAME/$2 2>/dev/null`
    if [[ ! -z "$EXCLUDE" ]]; then
        OUTPUT=`echo "$OUTPUT" | perl -pe "s/\"$EXCLUDE(_[0-9]*)?\"(.*[A-Z]+.*)?,? ?//g"`
    fi
    if [[ `echo "$OUTPUT" | grep 'ERROR' | wc -l` -gt 0 ]]; then
        log "Response from clustermanager: $OUTPUT" >&2
    fi
    echo "$OUTPUT"
}

function cm_post {
    local LOCATION=$1
    local COMMAND=$2
    shift 1

    required CLUSTERMANAGER LOCATION COMMAND

	URI="http://$CLUSTERMANAGER/$LOCATION"
	DATA="jsonParameters={\"command\":\""
	DATA="$DATA$COMMAND\""

	while [[ $# -gt 1 ]]; do
		DATA="$DATA,\"$1\":\"$2\""
		shift 2
	done

	DATA="$DATA}"

	curl -d "$DATA" -H "Content-Type: application/json" "$URI"
	echo
}

function get_resourceGroups {
    cm_get clusters resourceGroups | output_parser ResourceGroups
}

function get_partitions {
    local type=$1
    local RG=$2
    required type RG
    cm_get clusters resourceGroups/$RG/$type | awk '$1 ~ /\"'"$RG"'_[0-9]+\"/ {PARTITION=$1} $1 == "{" {DO="1"} $1 == "}" {DO="0"} DO == "1" && $3 ~ /(MASTER|SLAVE|ERROR|OFFLINE)/ {print $1" '"$RG"' "PARTITION" "$3}' | tr -d '",'
}

function get_instance_partitions {
    local type=$1
    local INSTANCE=$2
    required type INSTANCE
    shift 2
    
    local RESOURCEGROUPS=$*
    required RESOURCEGROUPS

    for rg in $RESOURCEGROUPS; do
        get_partitions $type $rg | awk '$1 ~ /'"$INSTANCE"'/ {print $2":"$3":"$4}'
    done
}

function filter_resourceGroups {
    local type=$1
    shift 1
    required type
    for RG in $@; do
        if [[ `cm_get clusters resourceGroups/$RG | awk '$1 ~ /"STATE_MODEL_DEF_REF"/ && $3 ~ /"'$type'"/' | wc -l` -eq 1 ]]; then
            echo $RG
        fi
    done
}

function partition_parser {
    awk 'PARTITION != "" {}'
}

function output_parser {
    awk '$1 == "\"'$1'\""' | grep -oE '\[.*\]' | tr -d '[] "' | tr ',' ' '
}

function check_counts {
    log "Checking instance counts"
    EXPECTED=$(cm_get zkPath INSTANCES | output_parser children | wc -w | tr -d ' ')
    log "  * Expected: $EXPECTED"
    if [[ $EXPECTED -eq 0 ]]; then
        log "ERROR! Instances is zero, cannot continue".
        do_exit 8
    fi
    LIVE=$(cm_get zkPath LIVEINSTANCES | output_parser children | wc -w | tr -d ' ')
    log "  * Live: $LIVE"
    if [[ $EXPECTED -ne $LIVE ]]; then
        log "ERROR!  One or more instances are down, cannot continue."
        do_exit 2
    fi
    wait_counts
}

function wait_counts {
    required RESOURCEGROUPS
    log "Waiting for externalview and idealstate counts to align..."
    for RG in $RESOURCEGROUPS; do
        ISMASTER=$(get_is $RG 'MASTER' | wc -l | tr -d ' ')
        ISSLAVE=$(get_is $RG 'SLAVE' | wc -l | tr -d ' ')
        if [[ $ISMASTER -eq 0 && $ISSLAVE -eq 0 ]]; then
            continue
        fi
        log "  * Resourcegroup $RG: Idealstate   MASTER[$ISMASTER] SLAVE[$ISSLAVE]"
        EVMASTER=0
        EVSLAVE=0
        while [[ $EVMASTER -ne $ISMASTER || $EVSLAVE -ne $ISSLAVE ]]; do
            EVMASTER=$(get_ev $RG 'MASTER' | wc -l | tr -d ' ')
            EVSLAVE=$(get_ev $RG 'SLAVE' | wc -l | tr -d ' ')
            log "                  $RG: ExternalView MASTER[$EVMASTER] SLAVE[$EVSLAVE]"
            sleep 1
        done
    done
}

function check_errors {
    local RESOURCEGROUPS=$*
    required RESOURCEGROUPS
    log "Checking partitions for errors"
    for RG in $RESOURCEGROUPS; do
        log -n "  * resourceGroup $RG: "
        if [[ `cm_get clusters resourceGroups/$RG/externalView | grep 'ERROR' | wc -l | tr -d ' '` -gt 0 ]]; then
            log "ERROR!  One or more partitions are in ERROR state, cannot proceed.  Aborting."
            do_exit 1
        fi
        echo "OK!"
    done
}

function get_is {
    required 1 2
    cm_get clusters resourceGroups/$1/idealState | grep $2
}

function get_ev {
    required 1 2
    cm_get clusters resourceGroups/$1/externalView | grep $2
}

function externalView_parser {
    sort -u | tr -d '", ' | cut -d':' -f1 | cut -d'_' -f1
}

function get_list_of_hosts {
    required CLUSTERMANAGER RESOURCEGROUPS
    log "Getting list of masters from $CLUSTERMANAGER"
    MASTERS=`get_masters $RESOURCEGROUPS`

    if [[ -z "$MASTERS" ]]; then
        log "ERROR: No masters found."
        do_exit 3
    fi

    echo "$MASTERS" | sed 's/^/                        \* /g'

    log "Getting list of slaves from $CLUSTERMANAGER"
    SLAVES=`get_slaves $RESOURCEGROUPS`

    if [[ -z "$SLAVES" ]]; then
        log "ERROR: No slaves found."
        do_exit 4
    fi

    echo "$SLAVES" | sed 's/^/                        \* /g'

    if [[ `echo "$SLAVES" | wc -l` < `echo "$MASTERS" | wc -l` ]]; then
        log "ERROR: Number of masters exceeds number of slaves."
        do_exit 5
    fi

    check_slave_lag $SLAVES
}

function get_slaves {
    for RG in $@; do
        get_ev $RG 'SLAVE'
    done | externalView_parser
}

function get_masters {
    for RG in $@; do
        get_ev $RG 'MASTER'
    done | externalView_parser
}

function get_live_instances {
    cm_get zkPath LIVEINSTANCES | output_parser children
}

function port_forward {
    destination_host=$1
    port=$2
    forward_host=$3
    required destination_host port forward_host
    log "Setting up port forwarding to $destination_host:$port via $forward_host (localhost:$port)..." >&2
        ( do_ssh -n -N -L $port:$destination_host:$port $forward_host 0</dev/null 1>/dev/null 2>/dev/null & echo $! )
        T=0
        M=10
        until nc -z localhost $port; do
                sleep 1
                ((T++))
                if [[ $T -gt $M ]]; then
                    log "Error with port forwarding." >&2
                    exit 1
                fi
        done > /dev/null
        log "Port forwarding process established." >&2
}

function glu_pw_prompt {
    required FABRIC
    if [[ -z $glupass ]]; then
        echo -n "Glu password: "
        stty -echo
        read glupass
        export glupass
        stty echo
        echo
    fi
    log "Checking glu password..."
    glu -f $FABRIC -x "$glupass" archived -t >/dev/null 2>&1 || (log "Invalid password or connectivity error to glu."; false) || do_exit 254
    log "Password validated."
}

function glu_cli {
    action=$1
    container=$2
    hosts=$3
    shift 3
    required FABRIC action container hosts
    log "Executing glu $action" $* "for $container on" $hosts
    if is_dry_run; then
        sleep 1
    else
        glu -f $FABRIC -x "$glupass" $action -P espresso $* --container $container `glu_host_options $hosts` || (log "Glu encountered an error, cannot continue."; false) || do_exit 253
    fi
    if [[ $action == "stop" ]]; then
        wait_for stop $hosts
    elif [[ $action == "start" ]]; then
        wait_for start $hosts
    fi
}

function check_slave_lag {
    log "Waiting for slave lag to recover..."
    for host in $@; do
        while true; do
            SECS=`do_ssh $host "/export/content/glu/apps/espresso-mysqld/i001/tmp/mysql/bin/mysql --socket=/export/content/data/mysql/i001/mysql.sock -e 'show slave status\G' -uespresso -pespresso" 2>/dev/null | grep Seconds_Behind_Master | awk '{print $2}'`
            log "  * $host: [$SECS]"
            if [[ $SECS != 'NULL' && $SECS -lt ${LAG_ALLOWED:-5} ]]; then
                break
            fi
            sleep 1
        done
    done
    log "...done."
}

function instance_enabled {
    local INSTANCE=$1
    required INSTANCE
    cm_get clusters instances/$INSTANCE | awk '/HELIX_ENABLED/ {print $3}' | tr -d '",'
}

function disable_instance {
    local INSTANCE=$1
    required INSTANCE
    if [[ $(instance_enabled $INSTANCE) == "true" ]]; then 
        log "Disabling instance $INSTANCE"
        if ! is_dry_run; then
            cm_post clusters/$CLUSTERNAME/instances/$INSTANCE enableInstance enabled false
        fi
    fi
}

function enable_instance {
    local INSTANCE=$1
    required INSTANCE
    if [[ $(instance_enabled $INSTANCE) == "false" ]]; then 
        log "Enabling instance $INSTANCE"
        if ! is_dry_run; then
            cm_post clusters/$CLUSTERNAME/instances/$INSTANCE enableInstance enabled true
        fi
    fi
}

function disable_controller {
    local CN=$1
    required CN
    log "Disabling controller for cluster $CN"
    if ! is_dry_run; then
        cm_post clusters/$CN/Controller enableCluster enabled false
    fi
}

function enable_controller {
    local CN=$1
    required CN
    log "Enabling controller for cluster $CN"
    if ! is_dry_run; then
        cm_post clusters/$CN/Controller enableCluster enabled true
    fi
}

function is_dry_run {
    if [[ $DRY_RUN -ne 1 ]]; then
        false
    fi
}

function wait_partitions {
    local ACTION=$1
    local INSTANCE=$2
    required INSTANCE ACTION
    shift 2

    RESOURCEGROUPS=$@
    required RESOURCEGROUPS

    log -n "Waiting for partitions to go $ACTION in $RESOURCEGROUPS..."
    if is_dry_run; then
        sleep 1
    else
        while true; do
            if [[ `get_instance_partitions externalView $INSTANCE $RESOURCEGROUPS | grep -v "$action" | wc -l` -eq 0 ]]; then
                break
            fi
            sleep 1
            echo -n "."
        done
    fi
    echo "done."
}

function wait_for {
    action=$1
    required action
    shift 1
    log -n "  * Waiting for hosts to $action in LIVEINSTANCES..."
    if is_dry_run; then
        sleep 1
        return
    fi
    if [[ "$action" =~ 'stop' ]]; then
        desired=0
    else
        desired=1
    fi  
    while true; do
        APPEARANCES=0
        LIVE=`get_live_instances $*`
        for host in $@; do
            if [[ "$LIVE" =~ "$host" ]]; then
                APPEARANCES=1
                break
            fi
        done
        if [[ $APPEARANCES -eq $desired ]]; then
            break
        fi  
        sleep 1
        echo -n "."
    done
    echo "done."
}

function do_ssh {
    ssh -oStrictHostKeyChecking=no $@ 2>/dev/null 
}

function backup_binlogs {
    do_ssh $1 'sudo -u app ls -l /export/content/data/mysql/i???/logs/*bin.??????'
    echoexec "do_ssh $1 sudo -u app bash -c 'for i in /export/content/data/mysql/i???/logs/*bin.??????; do gzip -v -c \$i > \$i.bak.gz; done 2>&1'"
    do_ssh $1 'sudo -u app ls -l /export/content/data/mysql/i???/logs/*bin.??????.bak.gz'
}

function glu_host_options {
    for host in $*; do
        echo -n "--host" $host " "
    done
}

function require_executable_exists {
    for file in $@; do
        if [[ ! -f $file ]]; then
            log "ERROR: Cannot find $file"
            exit 1
        elif [[ ! -x $file ]]; then
            log "ERROR: $file is not executable or you do not have execute permissions."
            exit 1
        fi
    done
}