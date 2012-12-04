#!/bin/bash

export JAVA_HOME=/export/apps/jdk/JDK-1_6_0_21/
export VIEW_ROOT=$HOME

function usage {
    echo
    echo "Usage: $0 -h"
    echo "       $0 [-d] -f FABRIC -n CLUSTER_NAME [-b INSTANCE_NAME] [-r INSTANCE_NAME] -c clusermanager_host:port [-a <hostname>] [-o <filename>]"
    echo
    echo "Options:"
    echo "  -h: These instructions"
    echo "  -f: Fabric name to operate against"
    echo "  -n: Name of espresso cluster to operate against"
    echo "  -c: Hostname:port of helix to operate against"
    echo "  -d: Dry run only: do not actually run actions"
    echo "  -a: Override default admin server to use for port forwarding (default: <fabric>-admin02.prod.linkedin.com)"
    echo "  -o: Output file: filename to output log entries to (in addition to stdout)"
    echo "  -b: Instance to take backup from (must be full hostname_port identifier; if not specified, -r will restore from latest backup taken)"
    echo "  -r: Instance to restore (must be full hostname_port identifier; if -b is specified also, backups will be taken from that instance prior to running the restore)"
    echo "  -t: Override default backup/restore script timeout with custom value (in seconds) "
    echo
    echo "Examples:"
    echo "  $0 -f PROD-ELA4 -n ESPRESSO_USCP -c ela4-app0727.prod.linkedin.com:12924 -b ela4-app2821.prod.linkedin.com_21401 -d"
    echo
}

while getopts "hf:n:c:da:o:b:r:t:" OPTION
do
  case $OPTION in
    b)  BACKUP="$OPTARG"
        BHOST=${BACKUP%_*};;
    r)  RESTORE="$OPTARG"
        RHOST=${RESTORE%_*};;
    o)  LOGFILE="$OPTARG"
        touch "$LOGFILE" || (echo "ERROR: Could not write to $LOGFILE"; false) || exit 1
        exec 1> >(tee -a "$LOGFILE") 2>&1;; 
    a)  ADMIN_SERVER="$OPTARG";;
    d)  DRY_RUN=1;;
    f)  FABRIC="$OPTARG";;
    n)  CLUSTERNAME="$OPTARG";;
    c)  CLUSTERMANAGER="$OPTARG"
        CMHOST=${CLUSTERMANAGER%%:*}
        CMPORT=${CLUSTERMANAGER#*:};;
    t)  TIMEOUT="$OPTARG";;
    h)  usage; exit 0;;
    \?) usage; exit 1;;
  esac
done

SCRIPTDIR=`dirname $0`
. "$SCRIPTDIR/espresso-bash-libs.inc.sh"

BACKUP_RESTORE_SCRIPT="$SCRIPTDIR/espresso_backup_restore_cmd.py"
CATCHUP_SCRIPT="$SCRIPTDIR/espresso_v08_catchup_after_restore.py"
require_executable_exists $BACKUP_RESTORE_SCRIPT $CATCHUP_SCRIPT

if [[ -z $FABRIC || -z $CLUSTERNAME || -z $CLUSTERMANAGER || (-z $BACKUP && -z $RESTORE)]]; then
    log "ERROR: -f, -n, and -c options are mandatory.  At least one of -b or -r must be specified as well."
    usage
    exit 1
fi

function partition_action {
    local ACTION=$1
    local INSTANCE=$2
    local PARTITION=`echo $3 | awk -F: '{print $2}' | cut -d_ -f2`
    local RG=`echo $3 | awk -F: '{print $1}'`
    required BACKUP_RESTORE_SCRIPT CMHOST CMPORT CLUSTERNAME INSTANCE RG PARTITION ACTION

    log "Running $ACTION for $PARTITION from cluster $CLUSTERNAME resourceGroup $RG on $INSTANCE via $CMHOST:$CMPORT"
    echoexec "$BACKUP_RESTORE_SCRIPT -c $ACTION --dbname $RG --partition_id $PARTITION --instance_name $INSTANCE --host $CMHOST --port $CMPORT --cluster_name $CLUSTERNAME -w --timeout ${TIMEOUT:=120}"
    if [[ $? -ne 0 ]]; then
        log "ERROR: Backup/restore script returned an error.  Halting!"
        do_exit 1
    fi
}

RESOURCEGROUPS=$(filter_resourceGroups MasterSlave `get_resourceGroups`)
log "Got MasterSlave resourcegroups: " $RESOURCEGROUPS

if [[ ! -z $BACKUP && ! -z $RESTORE ]]; then
    BACKUP_PARTITIONS=$(get_instance_partitions externalView $BACKUP $RESOURCEGROUPS)
    RESTORE_PARTITIONS=$(get_instance_partitions externalView $RESTORE $RESOURCEGROUPS)
    
    log "Verifying $BACKUP and $RESTORE contain the same partitions"
    if [[ `echo "$BACKUP_PARTITIONS" | awk '{print $2}' | md5sum | awk '{print$1}'` != `echo "$RESTORE_PARTITIONS" | awk '{print $2}' | md5sum | awk '{print$1}'` ]]; then
        log "ERROR: Partitions from $BACKUP and $RESTORE do not match.  Cannot continue."
        do_exit 1
    fi
fi

if [[ ! -z $RESTORE ]]; then
    if [[ `get_instance_partitions externalView $RESTORE $RESOURCEGROUPS | grep 'MASTER' | wc -l` -gt 0 ]]; then
        log "ERROR: One or more partitions are in MASTER state, cannot continue."
        do_exit 1
    fi

    disable_instance $RESTORE
    wait_partitions OFFLINE $RESTORE $RESOURCEGROUPS

    PARTITIONS=$(get_instance_partitions externalView $RESTORE $RESOURCEGROUPS)

    log "Backing up binlogs on $RHOST"
    backup_binlogs $RHOST
fi

if [[ ! -z $BACKUP ]]; then
    PARTITIONS=$(get_instance_partitions externalView $BACKUP $RESOURCEGROUPS)
fi

for partition in $PARTITIONS; do
    partition=${partition%:*}
    if [[ ! -z $BACKUP ]]; then
        log "Backing up $BACKUP:$partition"
        partition_action "BackupNext" $BACKUP $partition
    fi
    if [[ ! -z $RESTORE ]]; then
        log "Restoring $RESTORE:$partition"
        partition_action "Restore" $RESTORE $partition
    fi
done

if [[ ! -z $RESTORE ]]; then
    log "Catching up $RHOST..."
    echoexec "$CATCHUP_SCRIPT -i $RESTORE --cluster_name $CLUSTERNAME --helix_webapp_host_port $CLUSTERMANAGER"

    log "Restore complete.  You will need to enable and bounce storage instance $RESTORE manually."
else
    log "Backup complete."
fi

do_exit 0
