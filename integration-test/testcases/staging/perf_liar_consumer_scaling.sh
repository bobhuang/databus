#!/bin/bash
# test liar performance
#******************************************************
# set TEST_NAME before calling setup_env.inc
#******************************************************
export TEST_NAME=perf_liar_consumer_scaling_test

# sets up common environmnet variables and
source setup_env.inc

# Clean up current log
rm -f $SCRIPT_DIR/../var/log/perf/run_liar_consumer_scaling_test

# Number of clients
for nc in 1 10 25 50 100  
do
# Inner loop to vary prod_rate. The rate is in events/sec
for pr in 100 
do
# Clean up current host
echo "Clean up the existing processes"
jps | grep -i "Liar" | awk ' { print $1 }' | xargs kill -9

# Start the liar relay
echo "======Start the liar relay======"
./start_liar_relay_staging_setup_perf.test

# Keep the events streaming (so that consumer does not go down before stat collection )
echo "Setting event rate as $pr and running for 6mins"
sleep 60
$SCRIPT_DIR/dbus2_gen_event.py -e $pr -t 360000 -s 20 --keyMin=0 --keyMax=9999 --server_port=${relay_port} -b 10000000 & 
sleep 2

# Consumer setup
cp = 0 # Consumer poll interval = 0 ( no sleeps in between )
ssh esv4-be39.corp.linkedin.com cp=$cp nc=$nc PATH=$PATH 'bash -s' <<'ENDSSH'
# Remote terminal commands
jps | grep -i "Liar" | awk ' { print $1 }' | xargs kill -9
sleep 2
echo "======Start SSH session - Change directory to staging======"
cd ~/trunk/integration-test/testcases/staging
echo "======Print current directory======"
pwd
echo "Setting clientpollfreq as $cp, Num consumers as $nc"

nohup ./start_multiple_liar_consumer_staging_setup_perf.test $cp $nc > start_multiple_liar_consumer_staging_setup_perf.log 2> start_multiple_liar_consumer_staging_setup_perf.log < /dev/null
# Allow the client to come up ( to respond to REST calls )
echo "======Ending SSH session======"
ENDSSH

# Initiate statistics collection
echo "Collecting statistics after sleep 3"
sleep 3
$SCRIPT_DIR/perf_utils.py $cp $pr $nc

# Consumer setup
ssh esv4-be39.corp.linkedin.com cp=$cp nc=$nc PATH=$PATH 'bash -s' <<'ENDSSH'
echo "======Start SSH session - Change directory to staging======"
cd ~/trunk/integration-test/testcases/staging
nohup ./stop_multiple_liar_consumer_staging_setup_perf.test $cp $nc > stop_multiple_liar_consumer_staging_setup_perf.log 2> stop_multiple_liar_consumer_staging_setup_perf.log < /dev/null
jps | grep -i "Liar" | awk ' { print $1 }' | xargs kill -9
echo "======Ending SSH session======"
ENDSSH
# Stop the liar relay
echo "======Stop the liar relay after sleep 5======"
sleep 5
./stop_multiple_liar_relay_staging_setup_perf.test
done
sleep 5
done
