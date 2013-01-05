#!/bin/bash

cd `dirname $0`

./run_scrapers.sh `cat ela4-hosts.list`
