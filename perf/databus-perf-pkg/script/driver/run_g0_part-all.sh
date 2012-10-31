#!/bin/bash

cd `dirname $0`/../..

echo " ====> running with C=5"
script/driver/run_g0c5p10_part.sh
echo " ====> running with C=10"
script/driver/run_g0c10p10_part.sh
echo " ====> running with C=20"
script/driver/run_g0c20p10_part.sh
echo " ====> running with C=40"
script/driver/run_g0c40p10_part.sh
echo " ====> running with C=80"
script/driver/run_g0c80p10_part.sh
echo " ====> running with C=120"
script/driver/run_g0c120p10_part.sh

