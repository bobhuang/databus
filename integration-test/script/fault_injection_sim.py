#!/usr/bin/env python

'''
   Randomly suspends processes ( specified as input pids ), for short intervals of time
   simulating failed transactions/bounces of components
'''
__author__ = "Sajid Topiwala"
__version__ = "$Revision: 0.1 $"
__date__ = "$Date: 2011/07/13 $"

import sys, os
import pdb
import utility
from signal import signal, pause
from signal import SIGSTOP, SIGCONT
import time, copy, re
from optparse import OptionParser, OptionGroup
import random

# Global varaibles
options=None
maxDownTime=30
cycleTime=120

def resume(pid):
  print ( "pid: %s resume" % pid )
  os.kill(int(pid), SIGCONT)
  return
def suspend(pid):
  print ( "pid: %s suspend" % pid )
  os.kill(int(pid), SIGSTOP)
  return
     
def main(argv):
    global options
    global maxDownTime
    global cycleTime
    parser = OptionParser(usage="usage: %prog [options]")
    parser.add_option("-p", "--pids", action="store", dest="pids", 
                       help="Comma separated list of pids. Processes with these pids will randomly go down for short amount of time")
    parser.add_option("-t", "--time", action="store", dest="time", default="3600",
                       help="Time period in which to run this test. Processes with the pids specified will randomly go down during this time period")
 

    (options, args) = parser.parse_args()
    print("options.pids = %s" % options.pids)
    pid_array= options.pids.split(',');
    print pid_array

    for n in range(0, (int)(options.time)/cycleTime):
        downTime=random.randint(0,maxDownTime)
        upTime=cycleTime - downTime
        downPidIndex=random.randint(0,len(pid_array)-1)
        suspend(pid_array[downPidIndex])
        time.sleep(downTime)
        resume(pid_array[downPidIndex])
        time.sleep(upTime)

if __name__ == "__main__":
    main(sys.argv[1:])


