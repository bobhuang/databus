#!/usr/bin/env python
#
#
# Copyright 2013 LinkedIn Corp. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#

"""
  Get names of client registrations. Here are some examples:

  To get all the Multi-partition registrations:
      PROGNAME localhost:10566

  To get all the single-partition registrations:
      PROGNAME -s localhost:10566

  To get all multi-partition registrations with '-' as separator
      PROGNAME -F- localhost:10566

  NOTE:
      -F is ignored if -s is also specified
"""
from dbusUtils import *
from optparse import OptionParser
import json
import sys

def getMultiRegs(hostport, sepstring):
    out = {}
    mpregNames = getClientMpRegistrations(hostport)
    for mpreg in mpregNames:
        regsUnderMp = mpregNames[mpreg]
        for regName in regsUnderMp:
            out[sepstring.join([mpreg, regName])] = True
    return out

def makeOptionParser():
    # May be there is a better way to get the program name than sys.argv[0]
    progname=sys.argv[0]
    parser = OptionParser()
    parser.add_option("-s", "--single-partition-regs", help="Get single partition regs(default: multi-partition regs)", default=False, dest="single_partition", action="store_true")
    parser.add_option("-d", type="int", dest="dlevel", help="Run at this debug level(default 0)", default=0, action="store", metavar="DEBUG-LEVEL")
    parser.add_option("-F", dest="sepstring", help="Separator char for multi-partition-regname(default \'.\')", default='.', action="store", metavar="SeparatorString")
    parser.set_usage(progname + " [options] <hostport> \n" + __doc__.replace("PROGNAME", progname))
    return parser

#
# Main
#
# Parse options and translate
parser = makeOptionParser()
(options, args) = parser.parse_args()
setDebugLevel(options.dlevel)
single_partition = options.single_partition
sepstring = options.sepstring
if len(args) != 1:
    parser.print_usage()
    exit(1)
hostport = args[0]

if single_partition:
    regs = getClientSingleRegistrations(hostport)
else:
    regs = getMultiRegs(hostport, sepstring)

for reg in regs.keys():
    print reg
