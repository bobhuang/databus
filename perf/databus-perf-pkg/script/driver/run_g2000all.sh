#!/bin/bash

script_dir=`dirname $0`

for c in 5 10 20 40 50 80 100 ; do
	echo " ====> running with C=$c"
	${script_dir}/run_g2000c${c}p10.sh
	echo " ====> done running with C=$c"
done

