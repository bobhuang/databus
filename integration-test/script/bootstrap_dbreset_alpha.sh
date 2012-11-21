#!/bin/bash

source "$(dirname $0)/bootstrap_dbreset.sh"

# recreate user and database
$VIEW_ROOT/database/databus2-bootstrap/createUser/cr_databus2.sh

# recreate table
# ant -f $VIEW_ROOT/database/databus2-bootstrap/build.xml db.reinit
$VIEW_ROOT/database/databus2-bootstrap/bootstrap_db_reinit.sh
