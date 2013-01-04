# Count the number of events in the relay, and save the events in the relay 
# in a file named events_on_relay_<uniquestring>_<relayport>
# $1 DB name
# $2 is the number of partitions (buffers) for which we want to get an event count.
# $3 relay port number
# $4 some identifiable string used for file extension (indicating the step the test is on)
# Expects ${VIEW_ROOT} ${WORK_DIR_FROM_ROOT} and $SCRIPT_DIR to be set.
# Returns the number of events saved away
function count_events_on_relay {
    if [ ! $# == 4 ];
        then echo "WRONG NUMBER OF ARGS to count_events_on_relay";
        return -1;
    fi

    events_file=${VIEW_ROOT}/${WORK_DIR_FROM_ROOT}/events_on_relay_$4_$3
    if [ -f ${events_file} ]; then 
        rm -f ${events_file}
    fi
    touch ${events_file}
    maxPartId=`expr $2 - 1`
    for i in `seq 0 $maxPartId`; do
        $SCRIPT_DIR/curl_stream_relay.sh $3 $1 $i >> ${events_file} ;
    done
    nevents=`grep -v endOfPeriod ${events_file} |wc -l`
    echo "events in  ${events_file} = $nevents"
    return $nevents
}
