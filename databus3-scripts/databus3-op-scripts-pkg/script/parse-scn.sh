#!/bin/sh
# A script to translate a genid-offset into SCN or vice versa
# Usage: PROGNAME 9039644881
# Usage: PROGNAME 2 449710289

function usage() {
    echo "Translates a genid-offset pair into SCN or vice versa"
    echo "Usage: " $0 " <SCN> | <genid> <offset>"
    exit 1
}

function stopIfNonNumeric() {
    echo $1 | grep "^[0-9][0-9]*$" 1>/dev/null 2>&1
    if [ $? -ne 0 ] ; then
        usage
    fi
}

if [ $# -eq 1 ] ; then
    stopIfNonNumeric $1
    echo | awk -v O=$1 '{print rshift(O,32),and(O,0xffffffff);}'
elif [ $# -eq 2 ] ; then
    stopIfNonNumeric $1
    stopIfNonNumeric $2
    echo | awk -v f=$1 -v o=$2 '{print lshift(f,32)+o}'
else
    usage
fi
exit 0
