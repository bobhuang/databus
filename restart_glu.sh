#!/bin/bash
/usr/local/linkedin/bin/mint glu >& mint.out
retcode=$?
echo -e "======== DONE RESTARTING GLU ======"
exit $retcode
