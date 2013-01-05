#!/bin/bash

script_dir=`dirname $0`


echo " ====> running with C=5"
${script_dir}/run_g2000c5p10_part.sh
echo " ====> running with C=5"
#${script_dir}/run_g2000c5p10_part.sh
echo " ====> running with C=10"
${script_dir}/run_g2000c10p10_part.sh
echo " ====> running with C=10"
#${script_dir}/run_g2000c10p10_part.sh
echo " ====> running with C=20"
${script_dir}/run_g2000c20p10_part.sh
echo " ====> running with C=20"
#${script_dir}/run_g2000c20p10_part.sh
echo " ====> running with C=40"
${script_dir}/run_g2000c40p10_part.sh
echo " ====> running with C=40"
#${script_dir}/run_g2000c40p10_part.sh
echo " ====> running with C=80"
${script_dir}/run_g2000c80p10_part.sh
echo " ====> running with C=80"
#${script_dir}/run_g2000c80p10_part.sh
echo " ====> running with C=100"
${script_dir}/run_g2000c100p10_part.sh
echo " ====> running with C=100"
#${script_dir}/run_g2000c100p10_part.sh

