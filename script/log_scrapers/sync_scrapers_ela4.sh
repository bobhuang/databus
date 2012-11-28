#!/bin/bash

cd `dirname $0`

./sync_scrapers.sh `cat ela4-hosts.list`
