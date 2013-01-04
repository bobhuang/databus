#!/bin/sh
#
# Script to run part of the qualification for mysqld release.
# The other part is in the espresso tree.
#

function killmysql() {
    echo "Killing mysql processes. You may be asked for password here"
    proclist=`ps h -o pid,user,comm -Cmysqld,mysqld_safe`
    echo -E "$proclist"
    myid=`id -nu`
    if [ "X$proclist" = "X" ] ; then
        echo No mysqld processes to kill
        return
    fi
    uname=`echo -E "$proclist" | awk '{print $2;}' |head -1`
    killcmd="kill -9"
    if [ $uname != $myid ] ; then
        killcmd="sudo kill -9"
    fi
    pidlist=`echo -E "$proclist" | awk '{print $1;}'`
    echo Killing processes $pidlist
    $killcmd $pidlist
}

function killjava() {
    echo "Killing java processes"
    jps
    pids=`jps | awk '{if(($2!="Jps")&&(!match($2,"^org.eclipse.equinox.launcher"))&&($2!="Main")&&($2!="ZooInspector")) print $1;}'`
    if [ "X${pids}" = "X" ] ; then
        echo No processes to kill
        return
    fi
    echo Killing $pids
    kill -9 $pids
}

function rmlogs() {
   /bin/rm -rf ../../var/log
}

# Check if mysqlds are alive as per rpl-install.sh
# HACK ALERT: Will need to change if rpl-install changes.
function checkmysql() {
    for p in 3306 14100 29000 29001; do
        mysqladmin --port=$p --protocol=tcp -uroot "ping"
        if [ $? -ne 0 ] ; then
            echo Mysql not set up right.
            exit 1
        fi
    done
}

function setupmysql() {
   if [ ! -d esn ] ; then
       echo Cannot find directory esn. Are you in the right directory?
       exit 1
   fi
   cd esn;
   sudo sh -x ./rpl-install.sh
   cd -;
}

function usage() {
    echo "Usage: $0 [-i]"
    echo "	-i: Kill and re-start mysqld as needed for the tests"
    exit 1
}

#
# Main
#

# Change directory to where the script is.
cd `dirname $0`

TESTCASES="espresso_relay_5_3_3_test1.test"
TESTCASES="${TESTCASES} espresso_dbusmgr_5_3_3_test1.test"
TESTCASES="${TESTCASES} espresso_schema_5_3_3_test1.test"
TESTCASES="${TESTCASES} espresso_cluster_manager_1.test"
RESTART_MYSQL=0

#Parse options
USAGE="Usage: `basename $0` [-i]\n\t "
while getopts "i" OPT; do
    case "$OPT" in
        i)
            echo "Killing and re-starting mysqld instances"
            RESTART_MYSQL=1
            ;;
       \?)
            # getopts issues an error message
            usage
            ;;
    esac
done
# Remove the switches we parsed above.
shift `expr $OPTIND - 1`

# Kill stuff remainining from before.
killjava
if [ ${RESTART_MYSQL} -eq 1 ] ; then
    killmysql
    setupmysql
fi
checkmysql

passed=""
failed=""

for testcase in ${TESTCASES} ; do
    killjava
    rmlogs
    echo `date`:Starting test: $testcase
    ./$testcase
    if [ $? -eq 0 ] ; then
        echo `date`:$testcase PASSED
        passed="$passed,$testcase"
    else
        echo `date`:$testcase FAILED
        failed="$failed,$testcase"
    fi
done

status=0
echo =================================================================================
if [ "X$failed" = "$failed" ] ; then
    echo All tests passed.
else
    echo Tests that PASSED: $passed
    echo Tests that FAILED: $failed
    status=1
fi
echo =================================================================================

exit $status
