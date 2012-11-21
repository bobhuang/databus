#!/bin/bash

cd `dirname $0`/../..

echo " ====> running with C=5"
script/driver/run_g3000c5p10.sh
sleep 360s
echo " ====> running with C=5"
script/driver/run_g3000c5p10.sh
sleep 360s
echo " ====> running with C=10"
script/driver/run_g3000c10p10.sh
sleep 510s
echo " ====> running with C=10"
script/driver/run_g3000c10p10.sh
sleep 510s
echo " ====> running with C=20"
script/driver/run_g3000c20p10.sh
sleep 660s
echo " ====> running with C=20"
script/driver/run_g3000c20p10.sh
sleep 660s
echo " ====> running with C=40"
script/driver/run_g3000c40p10.sh
sleep 1260s
echo " ====> running with C=40"
script/driver/run_g3000c40p10.sh
sleep 1260s
echo " ====> running with C=80"
script/driver/run_g3000c80p10.sh
sleep 1260s
echo " ====> running with C=80"
script/driver/run_g3000c80p10.sh


echo " ====> Synching ... "
script/driver/sync_remote.sh
