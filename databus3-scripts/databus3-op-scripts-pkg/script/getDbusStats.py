#!/usr/bin/env python

"""
  Get the value of one stat variable for all the partitions. The variable may be
  from event statistics or consumer callback statistics, and can be retrieved from
  client inbound stats or relay inbound/outbound stats. All output is printed to 
  stdout (See note on -d below). Here are some example uses:

  To get timeSinceLastEvent for inbound events:
      PROGNAME localhost:11140 timeSinceLastEvent

  To get timeSinceLastAccess for outbound events:
      PROGNAME -o localhost:11140 timeSinceLastAccess

  To get numDataEvents for client events from multi-partition registrations:
      PROGNAME -c localhost:10566 numDataEvents

  To get numDataEvents for client events from single-partition registrations:
      PROGNAME -c -s localhost:10566 numDataEvents

  To get numEventsProcesssed for consumer callbcak stats from multi-partition client registrations:
      PROGNAME -c -l localhost:10566 numEventsProcesssed

  NOTE:
      -o is ignored if -c is specified.
      -s is ignored if -c is not specified.
      -l is ignored if -c is not specified.
      -d prints some debugging info to stderr
"""

# optparse is deprecated as of 2.7, but we use 2.6.6
from optparse import OptionParser
from dbusUtils import *
import json
#import urllib2
import sys

def getRelayPsources(hostport, direction):
    return fetchJson(hostport, "/".join(["containerStats", direction, "events/psources"]))

def getRelayPsourceEventStats(hostport, psrc, direction):
    return fetchJson(hostport, "/".join(["containerStats", direction, "events/psource", psrc]))

def getRelayEventStats(hostport, direction):
    psrcs = getRelayPsources(hostport, direction)
    stats = {}
    for psrc in psrcs:
        psrcStats = getRelayPsourceEventStats(hostport, psrc, direction)
        stats[psrc] = psrcStats
    return stats

def getSingleRelayEventStat(hostport, direction, statName):
    out = {}
    stats = getRelayEventStats(hostport, direction)
    for psrc in stats:
        psrcStats = stats[psrc]
        out[psrc] = psrcStats[statName]
    return out
 
def getClientRegStats(hostport, reg, whereeFrom):
    #return fetchJson(hostport, "/".join(["clientStats", "inbound/events/registration", reg]))
    return fetchJson(hostport, "/".join(["regops/stats/inbound", whereFrom, reg]))

def getSingleClientRegStatFromMpRegs(hostport, statName):
    out = {}
    mpregNames = getClientMpRegistrations(hostport)
    for mpreg in mpregNames:
        regNames = mpregNames[mpreg]
        for reg in regNames:
            stats = getClientRegStats(hostport, reg, "events")
            out[".".join([mpreg,reg])] = stats[statName]
    return out

def getSingleClientRegStatFromSingleRegs(hostport, statName, whereFrom):
    out = {}
    regs = getClientSingleRegistrations(hostport);
    for reg in regs:
        stats = getClientRegStats(hostport, reg, whereFrom)
        out[reg] = stats[statName]
    return out

#def getSingleClientRegStatFromSingleRegs(hostport, statName, whereFrom):
    # HACK to check if a registration is within an mpreg or not
    # Another way is to get each regops/registrations and then get
    # each registration as regops/registration/<name>, but that seems
    # to be too many REST calls.
    #allMpRegs = {}
    #mpregNames = getClientMpRegistrations(hostport)
    #for mpreg in mpregNames:
        #regNamesUnderMp = mpregNames[mpreg]
        #for regName in regNamesUnderMp:
            #allMpRegs[regName] = True
#
    #out = {}
    #regNames = fetchJson(hostport, "clientStats/registrations")
    #for reg in regNames:
        ## Make sure this is not an mpreg
        #if reg in allMpRegs:
            #continue
        #stats = getClientRegStats(hostport, reg, whereFrom)
        #out[reg] = stats[statName]
    #return out

def makeOptionParser():
    # May be there is a better way to get the program name than sys.argv[0]
    progname=sys.argv[0]
    parser = OptionParser()
    parser.add_option("-c", "--client", help="Get stats from client(default: from relay)", default=False, dest="client", action="store_true")
    parser.add_option("-s", "--single-partition-regs", help="Get stats from client single partition regs(default: multi-partition regs)", default=False, dest="single_partition", action="store_true")
    parser.add_option("-l", "--callbacks", help="Get stats for consumer callbacks(default event stats)", default=False, dest="callbacks", action="store_true")
    parser.add_option("-o", "--outbound", dest="outbound", help="Get stats from relay outbound buffers(default: from inbound)", default=False, action="store_true")
    parser.add_option("-d", type="int", dest="dlevel", help="Run at this debug level(default 0)", default=0, action="store", metavar="DEBUG-LEVEL")
    parser.set_usage(progname + " [options] <hostport> <statName>\n" + __doc__.replace("PROGNAME", progname))
    return parser

#
# Main
#

# Parse options and translate
direction="inbound"
whereFrom="events"
client = False
parser = makeOptionParser()
(options, args) = parser.parse_args()
if (options.outbound):
    direction="outbound"
if (options.callbacks):
    whereFrom="callbacks"
setDebugLevel(options.dlevel)
client = options.client
single_partition = options.single_partition
if len(args) != 2:
    parser.print_usage()
    exit(1)
hostport = args[0]
statName = args[1]

# Do the work and output stuff
if client:
    if (single_partition):
        keys = getSingleClientRegStatFromSingleRegs(hostport, statName, whereFrom)
    else:
        keys = getSingleClientRegStatFromMpRegs(hostport, statName)
else:
    keys = getSingleRelayEventStat(hostport, direction, statName)

if keys:
    for key in keys:
        print key + "." + statName + " = " + str(keys[key])
else:
    print "No match"
