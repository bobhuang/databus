#!/bin/bash

cd `dirname $0`

./sync_scrapers.sh `cat stg-hosts.list`
