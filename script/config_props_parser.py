#!/usr/bin/python2.6


import os
import sys
import xml.etree.ElementTree as ET


VERBOSE=False

def debug(msg):
  if VERBOSE == True:
    print msg

def readConfigProps(file, prop_name):
  tree = ET.parse(file)
  root = tree.getroot()
  #root is 'application'
  # next element is <configuration-compilation>
  cc = root[0]
  if cc.tag != "{urn:com:linkedin:ns:configuration:compiled:1.0}configuration-compilation": 
    debug(" CONFIGURAIONT-COMPILATION key did't MATCH")

  debug("text " + cc.text)
  #print "attrib:\n " 
  #for a in cc.attrib: 
  #  print a
  
  # we are interested in two props 'prop_name'.default and 'prop_name'.override
  prop_name_default = prop_name + '.default'
  prop_name_override = prop_name + '.override'

  debug("props:")
  props = dict()

  for pp in cc:
    val = str(pp.attrib.get("value"))
    name = str(pp.attrib.get("name"))
    debug("name: " + name  + "; val = " + val)

    if name == prop_name_default :
      debug("found prop " + name + ":" + str(pp))

      default_prop = pp.find('{urn:com:linkedin:ns:configuration:compiled:1.0}props')
      debug( "default: " + str(default_prop))

      #get all default entries
      getAllEntries(default_prop, props)
      
    if name == prop_name_override :
      debug ("found prop " + name + ":"  + str(pp))
      override_prop = pp.find('{urn:com:linkedin:ns:configuration:compiled:1.0}props')
      debug( "override: " + str(override_prop))
      #overrides
      getAllEntries(override_prop, props)

  debug( "Props size is " + str(len(props)))
  return props

def getAllEntries(prop, dict):
  all_entries = prop.findall("{urn:com:linkedin:ns:configuration:compiled:1.0}entry")
  debug( "ENTRIES:" + str(len(all_entries)))
  debug( "dict size:" + str(len(dict)))
  for e in prop:
    val = str(e.attrib.get("value"))
    key = str(e.attrib.get("key"))
    debug( "key=" + key + "; val=" + val)
    if dict.has_key(key) : 
      debug( "overriding key = " + key + ";Old value = " + dict[key] + ". New value=" + val)
    dict[key] = val


def main(argv):
  if len(argv) != 1:
    print "Usage ConfigProps.py config_file.xml"
    sys.exit(-1)

  print argv[0]
  props = readConfigProps(argv[0])
  for (name, val) in props.iteritems():
    print name +  "=" + val



if __name__ == "__main__" :
  main(sys.argv[1:])


