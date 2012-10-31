#!/usr/bin/env python

import sys, os
#import pdb
#import time, copy, re
from optparse import OptionParser, OptionGroup
from utility import *
import socket

#Globals
options=None

def do_compare_relay_buffers():
	global options
	dbg_print("Inside compare_relay_buffers: exp_parts = " + options.expected_partitions)
	if options.expected_partitions == None:
            print "Expected args 'expected_partitions' is missing" 
	    return RetCode.ERROR
        #if options.expected_partitions == "":
        #    options.expected_partitions = []
	#list of actual parts from the relay
	actual_parts = get_relay_buffers(options.relay_host, options.relay_port)
	dbg_print("relay parts %s" % (actual_parts))
	#list of expected parts from the args (comma separated, format DB_pid)
	expected_parts = options.expected_partitions.split(',') 

        ## Hack Hack, to compare empty list with a emtpy string need to do this
        if expected_parts[0] == '':
            expected_parts=[]
	dbg_print("exp_parts %s" % (expected_parts))
	# should be same size
	if len(expected_parts) != len(actual_parts):
            print "Error: actual and arg partitions do not match."
            return RetCode.ERROR
	# each partitions from actual list should be in the arg list
	for p in actual_parts:
		if not p in str(expected_parts):
			print "ERROR: Cannot find " + p  + " in " + options.expected_partitions
			return RetCode.ERROR
	return RetCode.OK

def do_verify_rpldbus_node():
    threadList = get_rpldbus_nodes(options.relay_host, options.relay_port)
    mysqlHostList = []
    if options.mysqlMaster != "None":
        mysqlHostList = options.mysqlMaster.split(',')
    ret = RetCode.OK

    for mysqlHost in mysqlHostList:
            findThreadName = get_rpldbus_threadname_from_hostname(mysqlHost)
            mysql_host_and_port = mysqlHost.split(':')
            mysqlHostName = ""
            mysqlPortName = ""
            if len(mysql_host_and_port) > 0:
                mysqlHostName = getfqdn(mysql_host_and_port[0])
            if len(mysql_host_and_port) > 1:
                mysqlPortNum = mysql_host_and_port[1]
            found = False
            for item in threadList.keys():
                val = threadList[item]
                print ("Item: %s" % item)
                print ("Value: %s" % val)
                if( item == findThreadName ):
                    found = True # we found the right thread.. but make sure the thread is in the correct state now
                    if val["liRplSlaveName"] != item:
                       ret = RetCode.ERROR
                       print("liRplSlaveName for thread %s does not match the value blob" % item)
                       break
                    if val["masterHost"] != mysqlHostName: # ensure hostname matches
                       ret = RetCode.ERROR
                       print("masterHost for Thread %s does not match. Expected %s, Found %s" % (item, mysqlHostName, val["masterHost"]))
                       break
                    if val["masterPort"] != int(mysqlPortNum): #ensure portnum patches
                       ret = RetCode.ERROR
                       print("masterPort for Thread %s does not match. Expected %s, Found %s" % (item, mysqlPortNum, str(val["masterPort"])))
                       break
                    if val["rplDbusUp"] != True: #ensure that the slave thread is up and running
                       ret = RetCode.ERROR
                       print("Thread %s is expected to be running, but rplDbusUp is %s" % (item, val["rplDbusUp"]))
                       break
                    del threadList[item]
                    
                # Regardless whether this is the thread we are looking for or not - we need to ensure that any running thread is not in error state
                if val["rplDbusError"] != False and val["rplDbusUp"] == True:
                    ret = RetCode.ERROR
                    print ("Thread %s rplDbusError is set to: %s, while slaveSQLThreadRunning is %s. Error reported: %s" % (item, val["rplDbusError"], val["rplDbusUp"], val["error"]))

            if( found == False ): #we did not find the thread we are looking for.. error out and return
               print ("Could not find rpl_dbus thread: %s" % findThreadName)
               ret = RetCode.ERROR
               break

    if (ret == RetCode.OK):
            #we found all thread we are looking for.. now lets make sure that any remaining slave threads are in "stopped" mode
            print "Checking that all extra threads are in STOPPED state"
            for remainingItem in threadList.keys():
                val = threadList[remainingItem]
                if val["slaveSQLThreadRunning"] == True:
                   print ("Thread %s is expected to be STOPPED, but slaveSQLThreadRunning = %s" % (remainingItem, val["slaveSQLThreadRunning"]))
                   ret = RetCode.ERROR
                   break

    sys.exit(ret)

def main(argv):
	global options, outf, args, input_files

	parser = OptionParser(usage="usage: %prog [options] file1 [file2]")
	parser.add_option("--relay_buffers_compare", action="store_true", dest="relay_buffers_compare", default = False,
	        help="espresso relay_buffers compare")
	parser.add_option("--relay_rpldbus_nodes_verify", action="store_true", dest="relay_rpldbus_nodes_verify", default = False,
	        help="espresso relay_rpldbus_nodes verify")
	parser.add_option("--relay_host", action="store", dest="relay_host", default = "localhost",
                       help="host where databus relay is running")
	parser.add_option("--relay_port", action="store", dest="relay_port", default = "11140",
                       help="port where databus relay is running")
	parser.add_option("--expected_partitions", action="store", dest="expected_partitions", default = "",
                       help="expected mapping between partition and relay")
        parser.add_option("--mysqlMaster", action="store", dest="mysqlMaster", default="localhost:3306", 
                       help="expected mysql master for thread. Used with the --relay_rpldbus_nodes_verify option")

        parser.add_option("--debug", action="store_true", dest="debug", default="false" )

	(options, args) = parser.parse_args()

	if options.debug :
		set_debug(True)

	if options.relay_rpldbus_nodes_verify:
                ret = do_verify_rpldbus_node()
                sys.exit(ret)

	
	ret = RetCode.OK
	if options.relay_buffers_compare:
		ret = do_compare_relay_buffers()
		sys.exit(ret)


	if ret == RetCode.OK: print "ok" 
	sys.exit(ret)
     
if __name__ == "__main__":
    main(sys.argv[1:])
