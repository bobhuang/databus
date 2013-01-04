#!/bin/bash

function usage {
    echo
    echo "Usage: $0 -h"
    echo "       $0 [-d] [-b] -f FABRIC -n CLUSTER_NAME -c clusermanager_host:port [-r <relay_cluster_name>] [-s] [-a <hostname>] [-i #] [-l #] [-x <regex>] [-o <filename>]"
    echo
    echo "Options:"
    echo "  -h: These instructions"
    echo "  -f: Fabric name to operate against"
    echo "  -n: Name of espresso cluster to operate against"
    echo "  -c: Hostname:port of helix to operate against"
    echo "  -r: Name of the relay cluster to pause prior to running the ugprade"
    echo "  -s: Upgrade _S_torage nodes only (or _S_kip mysql upgrades)"
    echo "  -d: Dry run only: do not actually run deploy actions"
    echo "  -b: Bounce only: don't actually upgrade or deploy software, just restart it"
    echo "  -i: iterations: Number of times to run upgrade (default 1)"
    echo "  -l: Slave lag (in seconds) below which replication will be considered \"caught up\" (default 5)"
    echo "  -x: eXclude regeX: A regular expression describing servers to be ignored"
    echo "  -o: Output file: filename to output log entries to (in addition to stdout)"
    echo
    echo "Examples:"
    echo "  $0 -f EI -n ESPRESSO_DEV_SANDBOX_2 -c eat1-app113.corp.linkedin.com:12924"
    echo "  $0 -f EI -n ESPRESSO_DEV_SANDBOX_2 -c eat1-app113.corp.linkedin.com:12924 -s"
    echo
}

while getopts "hsf:n:c:dbi:l:x:o:r:" OPTION
do
  case $OPTION in
    o)  LOGFILE="$OPTARG"
        touch "$LOGFILE" || (echo "ERROR: Could not write to $LOGFILE"; false) || exit 1
        exec 1> >(tee -a "$LOGFILE") 2>&1
        ;; 
    x)  EXCLUDE="$OPTARG";;
    l)  LAG_ALLOWED="$OPTARG";;
    i)  ITERATIONS="$OPTARG";;
    d)  DRY_RUN=1;;
    b)  BOUNCE_ONLY=1;;
    s)  SKIP_MYSQL=1;;
    f)  FABRIC="$OPTARG";;
    n)  CLUSTERNAME="$OPTARG";;
    c)  CLUSTERMANAGER="$OPTARG"
        CMHOST=${CLUSTERMANAGER%%:*}
        CMPORT=${CLUSTERMANAGER#*:};;
    r)  RELAY_CLUSTER_NAME="$OPTARG";;
    h)  usage; exit 0;;
    \?) usage; exit 1;;
  esac
done

SCRIPTDIR=`dirname $0`
. $SCRIPTDIR/espresso-bash-libs.inc.sh

# setup backup cmd path
BACKUP_RESTORE_SCRIPT="$SCRIPTDIR/espresso_backup_restore_cmd.py"
require_executable_exists $BACKUP_RESTORE_SCRIPT

if [[ -z $FABRIC || -z $CLUSTERNAME || -z $CLUSTERMANAGER ]]; then
    log "ERROR: -f, -n, and -c options are mandatory."
    usage
    exit 1
fi

function do_upgrade {
    check_errors $RESOURCEGROUPS
    if [[ $BOUNCE_ONLY -eq 1 ]]; then
        GLUACT='bounce'
    else
        GLUACT='redeploy'
    fi
    if [[ "$2" == '--parallel' ]]; then
        check_counts
        if [[ $SKIP_MYSQL -ne 1 ]]; then
            glu_cli stop espresso-storage-node "$1" $2
            check_errors $RESOURCEGROUPS
            glu_cli $GLUACT espresso-mysqld "$1" $2
            glu_cli $GLUACT espresso-storage-node "$1" $2
        else
            glu_cli $GLUACT espresso-storage-node "$1" $2
        fi
        check_errors $RESOURCEGROUPS
        check_counts
        check_slave_lag $1
    else
        if [[ $SKIP_MYSQL -ne 1 ]]; then
            for host in $1; do
                check_counts
                glu_cli stop espresso-storage-node "$host"
                check_errors $RESOURCEGROUPS
                glu_cli $GLUACT espresso-mysqld "$host"
                glu_cli $GLUACT espresso-storage-node "$host"
                check_errors $RESOURCEGROUPS
                check_counts
                if [[ $DRY_RUN -ne 1 ]]; then
                    check_slave_lag $host
                else
                    log "[DRY RUN] Not waiting for slave lag to recover for current masters"
                    sleep 1
                fi
            done
        else
            check_errors $RESOURCEGROUPS
            check_counts
            glu_cli $GLUACT espresso-storage-node "$1"
            check_errors $RESOURCEGROUPS
            check_counts
            if [[ $DRY_RUN -ne 1 ]]; then
                check_slave_lag $1
            else
                log "[DRY RUN] Not waiting for slave lag to recover for current masters"
                sleep 1
            fi
        fi
    fi
}

if [[ $ITERATIONS -lt 1 ]]; then
    ITERATIONS=1
elif [[ $ITERATIONS -gt 1 ]]; then
    log "Iterating through $ITERATIONS iterations."
    glu_pw_prompt
fi

for i in `seq 1 $ITERATIONS`; do
    if [[ $ITERATIONS -gt 1 ]]; then
        log "*** Iteration $i ***"
    fi
    RESOURCEGROUPS=$(filter_resourceGroups MasterSlave `get_resourceGroups`)
    log "Got MasterSlave resourcegroups: " $RESOURCEGROUPS

    if [[ $SKIP_MYSQL -eq 1 ]]; then
        log "** Skipping MySQL upgrades (storage node only mode) **"
    fi

    check_errors $RESOURCEGROUPS
    check_counts

    glu_pw_prompt

    get_list_of_hosts

    log "Disable Backup Scheduler"
    echoexec "$BACKUP_RESTORE_SCRIPT --wait_for_message_completion -c StopBackupScheduler  --instance_name % --host $CMHOST --port $CMPORT --cluster_name $CLUSTERNAME"

    if [[ ! -z $RELAY_CLUSTER_NAME ]]; then
        log "Pause relay cluster $RELAY_CLUSTER_NAME..."
        disable_controller $RELAY_CLUSTER_NAME
    fi

    log "Upgrading" $SLAVES "in parallel mode..."
    do_upgrade "$SLAVES" --parallel

    log "Upgrading "$MASTERS "iteratively..."
    do_upgrade "$MASTERS"

    if [[ ! -z $RELAY_CLUSTER_NAME ]]; then
        log "Unpause relay cluster $RELAY_CLUSTER_NAME..."
        enable_controller $RELAY_CLUSTER_NAME
    fi

    log "Enable Backup Scheduler"
    echoexec "$BACKUP_RESTORE_SCRIPT --wait_for_message_completion -c StartBackupScheduler  --instance_name % --host $CMHOST --port $CMPORT --cluster_name $CLUSTERNAME"

    log "Getting new host roles..."

    get_list_of_hosts

    log "Upgrade complete!"
done

do_exit 0
