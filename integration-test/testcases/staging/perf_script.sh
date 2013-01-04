#!/bin/bash
# test liar performance
#******************************************************
# set TEST_NAME before calling setup_env.inc
#******************************************************
export TEST_NAME=perf_test

# sets up common environmnet variables and
source setup_env.inc

# Clean up current log
rm -f $SCRIPT_DIR/../var/log/perf/run 

#Outer loop is for connectionRetries..initSleep for client in ms. Roughly consumer retry rate = 1000 / cp
for cp in 0 10 100
do
# Inner loop to vary prod_rate. The rate is in events/sec
for pr in 1 10 100 1000
do
# Clean up current host
echo "Clean up the existing processes"
jps | grep -i "Liar" | awk ' { print $1 }' | xargs kill -9

# Start the liar relay
echo "======Start the liar relay======"
./start_liar_relay_staging_setup_perf.test

# Keep the events streaming (so that consumer does not go down before stat collection )
echo "Setting event rate as $pr and running for 6mins"
sleep 120
$SCRIPT_DIR/dbus2_gen_event.py -e $pr -t 360000 -s 20 --keyMin=0 --keyMax=9999 --server_port=${relay_port} -b 200000000 & 
sleep 2

# Consumer setup
ssh esv4-be39.corp.linkedin.com cp=$cp PATH=$PATH 'bash -s' <<'ENDSSH'
# Remote terminal commands
jps | grep -i "Liar" | awk ' { print $1 }' | xargs kill -9
sleep 2
echo "======Start SSH session - Change directory to staging======"
cd ~/trunk/integration-test/testcases/staging
echo "======Print current directory======"
pwd
echo "Setting clientpollfreq as $cp"

nohup ./start_liar_consumer_staging_setup_perf.test $cp > start_liar_consumer_staging_setup_perf.log 2> start_liar_consumer_staging_setup_perf.log < /dev/null
# Allow the client to come up ( to respond to REST calls )
echo "======Ending SSH session======"
ENDSSH

# Initiate statistics collection
echo "Collecting statistics after sleep 3"
sleep 3
$SCRIPT_DIR/perf_utils.py $cp $pr 

# Consumer setup
ssh esv4-be39.corp.linkedin.com cp=$cp PATH=$PATH 'bash -s' <<'ENDSSH'
echo "======Start SSH session - Change directory to staging======"
cd ~/trunk/integration-test/testcases/staging
nohup ./stop_liar_consumer_staging_setup_perf.test $cp > stop_liar_consumer_staging_setup_perf.log 2> stop_liar_consumer_staging_setup_perf.log < /dev/null
jps | grep -i "Liar" | awk ' { print $1 }' | xargs kill -9
echo "======Ending SSH session======"
ENDSSH
# Stop the liar relay
echo "======Stop the liar relay after sleep 5======"
sleep 5
./stop_liar_relay_staging_setup_perf.test
done
sleep 5
done
