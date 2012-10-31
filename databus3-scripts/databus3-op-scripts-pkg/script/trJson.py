#!/usr/bin/env python
"""
  Reads JSON from stdin and finds a specific key and outputs it to stdout.
  The key be specified in a UNIX directory-like format, with 
  slashes. Leading slashes are implicit but can be specified explicitly.
  If an array is encountered in the path then the path component is applied
  to each element in the array.

  A message is printed in stderr if a path-element is not found (multiple
  messages are printed if the path element is not there in each array
  element)

  Assuming that PROGNAME is given the following as stdin:

   {
     "foo": "bar",
     "baz" : {
        "one": 1,
        "zoo": "zoo",
        "two": {
           "li": 1,
           "fb": null
        }
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

   NOTE:
     -p is ignored if -k is specified.

   TODO:
     If needed, improve the way arrays are handled.
"""
from dbusUtils import *
from optparse import OptionParser
import json
import sys

def printDict(elem):
    if keys:
        for k in elem.keys():
            print k
        return
    if pretty:
        print json.dumps(elem, sort_keys=True, indent=2)
    else:
        print json.dumps(elem)

def printList(elem):
    for o in elem:
        if type(o) is list or type(o) is dict:
            dprint(1, "list of lists or objs")
            printDict(o)
        else:
            dprint(1, "list of objs")
            print o

def printElement(elem):
    if type(elem) is dict:
        dprint(1, "Printing dict")
        printDict(elem)
    elif type(elem) is list:
        dprint(1, "Printing list")
        printList(elem)
    else:   # Hopefully is a primitive
        dprint(1, "Printing leaf")
        print elem

def findElement(obj, todo, done):
    dprint(1, str(type(obj)) + ",TODO=" + "/".join(todo) + ",DONE=" + "/".join(done))
    if type(obj) is dict:
        if todo[0] not in obj:
            print >> sys.stderr, "/".join(done) + "/" + todo[0] + " not present"
            return
        if len(todo) is 1:
            dprint(1, "Printing final element")
            printElement(obj[todo[0]])
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
    printElement(jsonIn)
    exit(0)

findElement(jsonIn, path, [])
