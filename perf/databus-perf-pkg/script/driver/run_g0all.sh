#!/bin/bash


for C in 5 10 20 40 80 100 200 ; do
	for P in 10 55 100 ; do
		d=`date`
		echo "********* Running C=${C} P=${P}: $d"
		./run_g0c${C}p${P}.sh
		d=`date`
		echo "********** Done C=${C} P=${P}: $d"
	done
done

