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
  Reads JSON from stdin and finds a specific key and outputs it to stdout.
  The key be specified in a UNIX directory-like format, with
  slashes. Leading slashes are implicit but can be specified explicitly.
  If an array is encountered in the path then the path component is applied
  to each element in the array.

  The key parts are treated as a regular expression (NOT glob) if the -r option is
  specified. See man regex(7)

  Entire path elements are also printed if -P is specified.

  A message is printed in stderr if a path-element is not found (multiple
  messages are printed if the path element is not there in each array
  element)

  Assuming that PROGNAME is given the following as stdin:

   {
     "foo": "bar",
     "for": "never",
     "baz" : {
        "one": 1,
        "zoo": "zoo",
        "two": {
           "li": 1,
           "fb": null
        }
     },
     "start" : {
         "one": 10
     },
     "cow": [1, 5, 7],
     "mad": [ "one", "two", "nine" ],
     "got": [ {"a":1}, {"a":4}, {"a":9}]
   }

   To get the value of foo:
     PROGNAME foo

   To get the value of "two" in pretty format:
    PROGNAME -p baz/two

   To get the value of "li":
     PROGNAME baz/two/li

   To get only the keys at top level:
     PROGNAME -k

   To get only the keys for "baz/two"
     PROGNAME -k baz/two
     PROGNAME -k /baz/two

   To print the value of "for" and "foo":
     PROGNAME -r /fo
     PROGNAME -r /fo.*

   To print the value of any key that has second letter as 'o':
     PROGNAME -r .o

   To print the value of "one" for any of the top keys:
     PROGNAME -r .*/one

   To print the path elements in the above output:
     PROGNAME -P -r .*/one

   NOTE:
     -p is ignored if -k is specified.

   TODO:
     If needed, improve the way arrays are handled.
"""
from dbusUtils import *
from optparse import OptionParser
import json
import sys
import re

def printDict(path, elem):
    if keys:
        for k in elem.keys():
            if printPathName:
                sys.stdout.write(path + ": ")
            print k
        return
    if pretty:
        if printPathName:
            sys.stdout.write(path + ": ")
        print json.dumps(elem, sort_keys=True, indent=2)
    else:
        if printPathName:
            sys.stdout.write(path + ": ")
        print json.dumps(elem)

def printList(path, elem):
    for o in elem:
        if type(o) is list or type(o) is dict:
            dprint(1, "list of lists or objs")
            printDict(path, o)
        else:
            dprint(1, "list of objs")
            if printPathName:
                sys.stdout.write(path + ": ")
            print  o

def printElement(path, elem):
    if type(elem) is dict:
        dprint(1, "Printing dict")
        printDict(path, elem)
    elif type(elem) is list:
        dprint(1, "Printing list")
        printList(path, elem)
    else:   # Hopefully is a primitive
        dprint(1, "Printing leaf")
        if printPathName:
            sys.stdout.write(path + ": ")
        print elem

def reMatch(pattern, value):
    dprint(1, "Trying to match value " + value + " to pattern " + pattern)
    if re.match(pattern, value):
        dprint(1, "value " + value + " matches pattern " + pattern)
        return True
    else:
        dprint(1, "value " + value + " does not match pattern " + pattern)
        return False

def findElementRegex(obj, todo, done):
    dprint(1, "REGEX:" + str(type(obj)) + ",TODO=" + "/".join(todo) + ",DONE=" + "/".join(done))
    if type(obj) is dict:
        found = False
        for k in obj.keys():
            dprint(1, "Testing key " + k + " in regex " + todo[0])
            if reMatch(todo[0], k):
                # Found the object in the keys. Treat it like a match:
                if len(todo) is 1:
                    dprint(1, "Printing final element")
                    printElement("/".join(done) + "/" + k, obj[k])
                else:
                    elem = obj[k]
                    done.append(k)
                    findElementRegex(elem, todo[1:], done)
                    del done[len(done)-1]
    elif type(obj) is list:
        dprint(1, "Expanding list")
        for o in obj:
            findElementRegex(o, todo, done)

def findElement(obj, todo, done):
    dprint(1, str(type(obj)) + ",TODO=" + "/".join(todo) + ",DONE=" + "/".join(done))
    if type(obj) is dict:
        if todo[0] not in obj:
            print >> sys.stderr, "/".join(done) + "/" + todo[0] + " not present"
            return
        if len(todo) is 1:
            dprint(1, "Printing final element")
            printElement("/".join(done) + "/" + todo[0], obj[todo[0]])
            return
        elem = obj[todo[0]]
        done.append(todo[0])
        findElement(elem, todo[1:], done)
    elif type(obj) is list:
        dprint(1, "Expanding list")
        for o in obj:
            findElement(o, todo, done)

def makeOptionParser():
    # May be there is a better way to get the program name than sys.argv[0]
    progname=sys.argv[0]
    parser = OptionParser()
    parser.add_option("-P", "--print-path", help="Print path name as well (default: print value only) ", default=False, dest="printPathName", action="store_true")
    parser.add_option("-r", "--regex", help="Use regex when comparing strings (default: use raw string compare) ", default=False, dest="useRegex", action="store_true")
    parser.add_option("-k", "--keys", help="Print only the keys under the path(default: print objects) ", default=False, dest="keys", action="store_true")
    parser.add_option("-p", "--pretty", help="Pretty print if priting json object(default: no) ", default=False, dest="pretty", action="store_true")
    parser.add_option("-d", type="int", dest="dlevel", help="Run at this debug level(default 0)", default=0, action="store", metavar="DEBUG-LEVEL")
    parser.set_usage(progname + " [options] [path]\n" + __doc__.replace("PROGNAME", progname))
    return parser

#
# Main
#
# Parse options and translate
parser = makeOptionParser()
(options, args) = parser.parse_args()
setDebugLevel(options.dlevel)
pretty = options.pretty
keys = options.keys
useRegex = options.useRegex
printPathName = options.printPathName

if len(args) > 1:
    parser.print_usage()
    exit(1)
path = []
if len(args) == 1:
    path = str.split(args[0], "/")
    # Remove '' from path, in case we have a leading slash or // in the path.
    path = filter(lambda p: p != '', path)

# Read the input json
jsonIn = json.load(sys.stdin)
if len(path) is 0:
    printElement("", jsonIn)
    exit(0)

if useRegex:
    findElementRegex(jsonIn, path, [])
else:
    findElement(jsonIn, path, [])
