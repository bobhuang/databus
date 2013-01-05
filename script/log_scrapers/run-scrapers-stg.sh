#!/bin/bash

cd `dirname $0`

./run_scrapers.sh `cat stg-hosts.list`
