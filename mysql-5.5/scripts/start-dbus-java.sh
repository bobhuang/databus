#! /bin/sh

exit 0

cd ~/databus2/trunk
dist/databus2-relay-cmdline-pkg/bin/stop-espresso-relay.sh >/dev/null 2>&1 
dist/databus2-relay-cmdline-pkg/bin/start-espresso-relay.sh >/dev/null  2>&1 
sleep 5
