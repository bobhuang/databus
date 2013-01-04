#!/bin/bash
## Nuke all non standard mysql installs of rpl dbus

echo **** Must be SUDO to run this script ****
ps -ef | grep -i mysql | grep defaults | awk '{ print $2; } a' | xargs kill -9
rm -rf /export/apps/mysql-*


