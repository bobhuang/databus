#!/bin/bash

expid=$1
relay=$2
source=$3
lport=$4
buffer=$5
poll=$6
duration=$7
ts=$8
part=$9

result_file=result-$expid-$lport.$ts.out
pid_file=run-$expid-$lport.$ts.pid

script_dir=`dirname $0`
source ${script_dir}/setup.inc

date

if [ ! -z "$part" ] ; then
	partstr="--mod_part $part"
fi

readBufferSize=$((buffer))

# random start
sleep $((RANDOM % 10))

${script_dir}/../bin/dtail --stats $partstr -s "$source" -R "$relay" -F NOOP -u "$duration" -c "dtail.connectionDefaults.eventBuffer.maxSize=$buffer;dtail.connectionDefaults.eventBuffer.readBufferSize=$readBufferSize;dtail.connectionDefaults.pullerRetries.initSleep=$poll;dtail.container.httpPort=$lport;dtail.container.readTimeoutMs=5000;dtail.container.writeTimeoutMs=5000;dtail.pullerBufferUtilizationPct=80" -o ${result_file}  &
dtail_pid=$!
echo "dtail_pid=${dtail_pid}"
echo ${dtail_pid} > ${pid_file}

sleep 5s

java_pid=`ps h --ppid ${dtail_pid} -o pid`
echo "dtail_pid=${dtail_pid} --> java_pid=${java_pid}"
 
top -b -d 10 -n 55 -p ${java_pid} > top-$expid-$lport.$ts.out 2>&1 & 
 
sleep $duration
sleep 5s
#kill `cat ${pid_file}`

kill ${java_pid}
kill ${dtail_pid}

sleep 5s

if ps ${java_pid} ; then
  echo "dtail ${java_pid} failed to stop. getting stack dump..."
  ${JAVA_HOME}/bin/jstack ${java_pid} 2>&1 > stack-${expid}-$lport.$ts.txt
  kill -9 ${dtail_pid}
  kill -9 ${java_pid}
fi


