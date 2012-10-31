#!/bin/bash
# a helper script to make sure that mysqld is started with a reasonable max number of
# threads/processes

ulimit -u 16357
setup-rpl-dbus-slave