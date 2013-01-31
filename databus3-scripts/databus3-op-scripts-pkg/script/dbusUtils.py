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
  Utilities for python scripts. Exports the following:
  dlevel, dprint, fetchJson
"""
import urllib2
import json
import sys

"""Runtime value of debug level (default 0)"""
dlevel = 0

def setDebugLevel(x):
    global dlevel
    dlevel = x

def dprint(printLevel, x):
    """
    Print the string x to stderr as long as dlevel (int) is greater than printLevel (int)
    """
    if (dlevel >= printLevel):
        print >> sys.stderr, "DEBUG: " + x

def fetchJson(hostport, uri):
    """
    Fetch a uri (string) from a given hostport (string) as Json
    Returns the response as a json object.
    """
    url = ("http://%s/%s" % (hostport, uri))
    dprint(1, url)
    resp = urllib2.urlopen(url).read()
    respJson = json.loads(resp)
    dprint(2, json.dumps(respJson))
    return respJson

def getClientMpRegistrations(hostport):
    """
    Returns all the mpregistrations with their sub-registration names, in a format like this:
    {
      "multiPartitionConsumer":[
        "multiPartitionConsumer_EspressoDB8_0",
        "multiPartitionConsumer_EspressoDB8_1",
        "multiPartitionConsumer_EspressoDB8_2",
        "multiPartitionConsumer_EspressoDB8_3",
        "multiPartitionConsumer_EspressoDB8_4",
        "multiPartitionConsumer_EspressoDB8_5",
        "multiPartitionConsumer_EspressoDB8_6",
        "multiPartitionConsumer_EspressoDB8_7"]
    }
    """
    return fetchJson(hostport, "/".join(["clientStats", "mpRegistrations"]))

def getClientSingleRegistrations(hostport):
    allMpRegs = {}
    mpregNames = getClientMpRegistrations(hostport)
    # There can be several multi-partition registrations, each one of them
    # having several single-partition registrations under it. Walk these to
    # get one list of all registration names that belong to multi-partition
    # registrations.
    for reg in mpregNames:
        regsUnderMp = mpregNames[reg]
        for regName in regsUnderMp:
            allMpRegs[regName] = True

    allRegs = fetchJson(hostport, "clientStats/registrations")
    # The above URL gives us the names of all registrations, but we want to
    # remove the multi-partition registrations from these to get only the 
    # single-partition registrations.
    for reg in allRegs.keys():
        if reg in allMpRegs:
            del allRegs[reg]
    return allRegs
