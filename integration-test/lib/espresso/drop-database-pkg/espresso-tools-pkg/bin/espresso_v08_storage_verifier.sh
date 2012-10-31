#!/bin/bash

SCRIPTDIR=`dirname $0`
. "$SCRIPTDIR/espresso-bash-libs.inc.sh"

function usage {
    echo
    echo "Usage: $0 -h"
    echo "       $0 [-v|-q] [-r] HOSTNAME"
    echo
    echo "Options:"
    echo "  -h: These instructions"
    echo "  -v: Enable verbose output"
    echo "  -q: Enable quiet output (only outputs errors)"
    echo "  -r: Enable remediation"
    echo
    echo "Examples:"
    echo "  $0 -v ela4-app2821.prod.linkedin.com"
    echo
}

while getopts "hqrv" OPTION
do
  case $OPTION in
    r)  REMEDIATE=1;;
    v)  VERBOSE=1
        QUIET=0;;
    q)  QUIET=1
        VERBOSE=0;;
    h)  usage; exit 0;;
    \?) usage; exit 1;;
  esac
done

shift $((OPTIND-1))

if [[ -z $1 ]]; then
    log "ERROR: You must specify a hostname."
    exit 1
fi

function verify_remote {
    RESULTS=$(do_ssh $1 "$3" 2>&1)
    STATUS=$?
    
    if [[ $VERBOSE -eq 1 ]]; then
        log
        log_lines "$2" "$RESULTS"
        log
    elif [[ $QUIET -ne 1 ]]; then
        log "$2"
    fi

    if [[ $STATUS -eq 0 ]]; then
        if [[ $QUIET -eq 0 ]]; then
            log "   SUCCESS: $4"
        fi
        return $STATUS
    else
        log " * ERROR! $5"
        if [[ $REMEDIATE -eq 1 && ! -z "$6" ]]; then
            log "   Remediating with '$6'"
            do_ssh -t -y $1 "$6"
        elif [[ $REMEDIATE -eq 1 && -z "$6" ]]; then
            log "Cannot remediate automatically!"
            exit $STATUS
        fi
        return $STATUS
    fi
}

function check_packages {
    local HOSTNAME=$1
    shift 1
    
    for package in $@; do
        verify_remote $HOSTNAME "Checking for package $package..." "rpm -qi $package 2>&1" "Package $package found." "Package $package not found!" "sudo yum -y install $package"
    done
}

log "Running storage node verification checks on $1"

verify_remote $1 "Checking server availability..." "ping -c 1 $1" "$1 reachable." "Could not ping $1!"
verify_remote $1 "Checking backup automount mountpoint..." "stat /mnt/x001/*/espresso" "Mountpoint is good." "Mountpoint not found!"
if [[ $? == 0 ]]; then
    MOUNTPOINT=$(do_ssh $1 stat /mnt/x001/*/espresso | awk '$1 == "File:" {print $2}' | tr -d "\`'")
    verify_remote $1 "Checking backup automount operation..." "mount | grep '$MOUNTPOINT'" "Mount is good." "Filer not mounted!"
fi
verify_remote $1 "Checking proper backup symlink in place..." 'FABRIC=`hostname|cut -d- -f1|tr '[a-z]' '[A-Z]'`; test `readlink /export/content/data/backuprestore/nfsMount` == "/mnt/x001/$FABRIC/espresso"' "Symlink exists and points to filer mountpoint." "Symlink does not exist or does not point to the proper location."
verify_remote $1 "Checking for JDK 1.6.0.27..." "ls -ld /export/apps/jdk/JDK-1_6_0_27" "JDK 1.6.0.27 found." "JDK 1.6.0.27 not found!" "sudo yum -y install Linkedin-JDK6"
check_packages $1 pv pigz iotop numactl espresso-perf-monitoring
verify_remote $1 "Checking swap space..." "free; test \$(free | awk '\$1 == \"Swap:\" {print \$2}') -gt \$(free | awk '\$1 == \"Mem:\" {print \$2}')" "Swap space is in excess of total memory." "Insufficient swap available!" 
verify_remote $1 "Checking swappiness..." "/sbin/sysctl vm.swappiness; test \$(/sbin/sysctl vm.swappiness | awk '{print \$3}') -eq 0" "Swappiness is set properly." "Swappiness is not set properly!"
