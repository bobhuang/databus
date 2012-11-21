#!/bin/bash

source "$(dirname $0)/bootstrap_dbreset.sh"

# recreate user and database
# recreate table

$VIEW_ROOT/build/databus2-bootstrap/exploded/bin/reinit
