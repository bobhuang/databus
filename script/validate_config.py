#!/usr/bin/python2.6

import config_props_parser
import sys
from optparse import OptionParser



#some properties that must be defined with these(in most cases) values
#may be moved into a separate file (profile)
must_have_props = {
  "databus.relay.espressoSchemas.mode" : "zk",
  "databus.relay.container.tcp.enabled" : "true",
  "databus.relay.eventBuffer.allocationPolicy" : "MMAPPED_MEMORY",
  "databus.relay.clusterManager.enabled" : "true",
  "databus.relay.clusterManager.relayReplicationFactor" : "3",
  "databus.relay.eventBuffer.restoreMMappedBuffers" : "true",
  "databus.relay.eventBuffer.restoreMMappedBuffersValidateEvents" : "false",
  "databus.relay.rplDbusManager.enabled" : "true",
  "databus.relay.rplDbusManager.rplDbusMysqlPassword" : "espresso",
  "databus.relay.rplDbusManager.rplDbusMysqlUser" : "rplespresso"
}


#per fabric properties - TODO. might be moved into a separate profile file
EI_ZK="eat1-app207.stg.linkedin.com:12913,eat1-app208.stg.linkedin.com:12913,eat1-app209.stg.linkedin.com:12913"
fabric_props = {
    "ei" : {
            "databus.relay.espressoSchemas.zkAddress" : EI_ZK,
            "databus.relay.clusterManager.relayZkConnectString" : EI_ZK,
            "databus.relay.clusterManager.storageZkConnectString" : EI_ZK,
            "databus.relay.rplDbusManager.relayZkConnectString" : EI_ZK
    }
}

#depricated properties that should NOT be defined
must_remove_props = {
  "databus.relay.espressoDBs" : ""
}


#some global settings - should be in a profile too (per fabric for example)
MAX_DEBUG_LEVEL=10
G_VERBOSE=False
BASE_HTTP_PORT=28100
BASE_TCP_PORT=28000
BASE_RPL_PORT=29000
DIR_PREFIX='/export/content/data'
PROP_NAME_PREFIX = "databus2.relay.espresso"


def debug_print(level, message):
  #level currently is ignored
  if G_VERBOSE:
    print message

#define options and parse them
def parse_args(argv):
  parser = OptionParser(usage="usage: %prog [options] xml_file", description="validates config")
  parser.add_option("-c", "--cluster_name", action="store", type="string", dest="cluster_name", default=None, help="full cluster name. e.g. DEV_FT_4")
  parser.add_option("-s", "--service_name", action="store", type="string", dest="service_name", default=None, help="service name. e.g. databus3-relay-espresso")
  parser.add_option("-f", "--fabric", action="store", type="string", dest="fabric", default=None, help="Fabric to deploy to")
  parser.add_option("-H", "--hosts", action="store", type="string", dest="hosts", default=None, help="A comma-separated list of hosts to harvest")
  parser.add_option("-t", "--tag", action="store", type="string", dest="tag", default=None, help="the cluster tag to use")
  parser.add_option("-i", "--instance", action="store", type="int", dest="instance", default="1", help="instance num to validate; Default: 1")
  parser.add_option("-v", "--verbose", action="store_true", dest="verbose", help="verbose logging")
  parser.add_option("-d", "--debug", action="store_true", dest="debug_log", help="debug logging")
  parser.add_option("", "--parse_file_name", action="store_true", dest="parse_file_name", help=" use file name to figure out options")
  parser.add_option("", "--size_data", action="store", dest="size_data", help=" params to calculate sizees, format=qps,event_size_bytes,retention_hours,partitions")

   # Parse options
  (options, args) = parser.parse_args()
  if options.verbose :
    G_DEBUG_LEVEL = MAX_DEBUG_LEVEL
    G_VERBOSE = True
    config_props_parser.VERBOSE=True   
  print ("options: %s args: %s" % (options, args))

  return (options, args)


#figure out correct values
def buildExpectedValues(ops):
  debug_print(2, ">>>>>>>>>>>>>>>> builiding expected values")
  exp_props = dict()

  #convert instance number to instance name (1=>i001)
  instance_name = "i%03d"%(ops.instance)
  debug_print(2,"instance name = " + instance_name)

  # 1. cluster name
  key = 'databus.relay.rplDbusManager.relayClusterName'
  exp_props[key] = "RELAY_"+ ops.cluster_name
  key = 'databus.relay.clusterManager.relayClusterName'
  exp_props[key] = "RELAY_"+ ops.cluster_name
  key = 'databus.relay.clusterManager.storageClusterName'
  exp_props[key] = "ESPRESSO_"+ ops.cluster_name
  key = 'databus.relay.espressoSchemas.rootSchemaNamespace'
  exp_props[key] = "schemas_registry/ESPRESSO_" + ops.cluster_name
  
  #ports
  http_port = BASE_HTTP_PORT + ops.instance
  tcp_port = BASE_TCP_PORT + ops.instance
  rplDbusPort = BASE_RPL_PORT + ops.instance
 
  key = 'databus.relay.container.httpPort'
  exp_props[key] = http_port
  key = 'databus.relay.container.tcp.port'
  exp_props[key] = tcp_port
  key = 'databus.relay.rplDbusManager.relayRplDbusMapping'
  exp_props[key] = "useLocalHostName_%d:useLocalHostName_%d" % (http_port,rplDbusPort)
  key = 'databus.relay.clusterManager.instanceName'
  exp_props[key] = "useLocalHostName_%s"%(http_port)
  key = 'databus.relay.rplDbusManager.instanceName'
  exp_props[key] = "useLocalHostName_%s"%(http_port)


  #must-have values
  debug_print(1, " must have values:")
  for (k,v) in must_have_props.items():
    debug_print(1, k)
    exp_props[k]=v

  #directories
  key='databus.relay.dataSources.sequenceNumbersHandler.file.scnDir'
  exp_props[key]="%s/%s/%s/binlog_ofs"%(DIR_PREFIX,ops.service_name,instance_name)
  key='databus.relay.eventBuffer.mmapDirectory'
  exp_props[key]="%s/%s/%s/mmap"%(DIR_PREFIX,ops.service_name, instance_name)

  #fabric specific keys
  debug_print(1, "fabric specific keys:")
  fabric = ops.fabric.lower()
  for (k,v) in fabric_props[fabric].items():
    debug_print(1, k)
    exp_props[k] = v

  
  return exp_props

#figure out buffer sizes based on qps and retention
def calculateBufferSizes(ops):
  if not ops.size_data:
    debug_print(1, "cannot calculate sizes without sizedata")
    return

  (qps,event_size,retention,partitions) = ops.size_data.split(",")
  print "qps=%s,event_size=%s,retention=%s,partitions=%s"%(qps,event_size,retention,partitions)
  min_props = dict()
  key="databus.relay.eventBuffer.maxSize"
  data_per_hour = int(qps)*int(event_size)*3600
  buf_size = data_per_hour*int(retention)/int(partitions)
  min_props[key] = buf_size
  key = "databus.relay.eventBuffer.scnIndexSize"
  min_props[key] = buf_size/256
  key = "databus.relay.eventBuffer.readBufferSize"
  min_props[key] = int(event_size)*1000 #not a good estimate. what is good?
  
  return min_props

#validate optons
def validateOptions(ops):
  if(ops.service_name == None):
    print "service name not specified" 
    sys.exit(-3)
  if(ops.cluster_name == None):
    print "cluster_name name not specified" 
    sys.exit(-3)
  if(ops.instance == None):
    print "instance name not specified" 
    sys.exit(-3)


# compare the values in expected props to real ones
def validate(exp_props, min_props, props):
  global G_VERBOSE
  #put stuff in 3 dict - not_validated, match, missmatch
  mismatch = dict()
  skipped = dict()
  match = dict()
  missing = dict()

  debug_print(2,"Expected:")
  for (name, val) in exp_props.iteritems():
    debug_print(2, name +  "=" + str(val))
  debug_print(2, "REAL:")
  for (name, val) in props.iteritems():
    debug_print(2, name +  "=" + val)

  debug_print(1,  ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>VALIDATION>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
  for (k, v) in props.iteritems():
    if k in exp_props:
      exp_v = exp_props[k]
      if str(exp_v) != str(v):
        mismatch[k] = "k=%s; actual_v=%s != expected_v=%s"%(k,v,exp_v)
      else:
        match[k] = "k=%s; actual_v=%s == expected_v=%s"%(k,v,exp_v)

      del exp_props[k]

    elif k in min_props:
      #now go over min_props - real prop should be at least of that value
      min_val = min_props[k]
      if float(v) < float(min_val):
         mismatch[k]="k=%s; actual_val:%s < min_val: %s"%(k,v,min_val)
      else:
         match[k]="k=%s; actual_val:%s = min_val: %s"%(k,v,min_val)

      del min_props[k]
    else:
      skipped[v] = "k=%s; val=%s"%(k,v)
      continue

  #identify missing configs
  for (k,v) in exp_props.iteritems(): 
    missing[k]="k=%s; v=%v"%(k,v)
  for (k,v) in min_props.iteritems(): 
    missing[k]="k=%s; v=%v"%(k,v)

  return (mismatch, match, skipped, missing)


def output_results(res):
  #print the results - all keyed by file name
  print ">>>>>>>>>>>>>>>>>>>> RESULTS <<<<<<<<<<<<<<<<<<<<<<\n"
  for f in res.keys():
    (mismatch, match, skipped, missing) = res[f]
    print "======FILE:" + f
    print "======DON'T MATCH: "
    for (k,v) in mismatch.iteritems():
      print "\t" + v
    if G_VERBOSE:
      print "======MATCH: "
      for (k,v) in match.iteritems():
        print "\t" + v
    if G_VERBOSE:
      print "=====SKIPPED: "
      for (k,v) in skipped.iteritems():
        print "\t" + v
    print "=======MISSING: "
    for (k,v) in missing.iteritems():
      print "\t" + v

    print 


#parse file name  and figure out some of the options
def figure_out_options(file, ops):
  if file == None or not file:
    debug_print(1, "cannot derive options from empty file name")
    return

  file_parts = file.split("/")
  if len(file_parts) < 6:
    debug_print(1, "cannot derive options from this file name(not enough elements): " + file)
    return
    
  #e.g. databus3-relay-espresso-war/EI/databus3-relay-espresso-RELAY_USCP_PERF_TEST/eat1-app65/i003/instance.cfg
  l = len(file_parts)
  instance_name = file_parts[l-2]
  ops.instance = int(instance_name[1:])
  host = file_parts[l-3]
  cluster_name_extra = file_parts[l-4]
  ops.fabric = file_parts[l-5]
  war = file_parts[l-6]
  ops.service_name = war[:-4] # without the '-war'
  first_underscore_index = cluster_name_extra.find("_")
  ops.cluster_name = cluster_name_extra[first_underscore_index+1:]

  if G_VERBOSE:
    print "AFTER PARSING ops are:" 
    print ops

# for each file
def validateOneFile(file_name, options):
  debug_print(1, "file = " + file_name + "; key = " + PROP_NAME_PREFIX)
  all_props = config_props_parser.readConfigProps(file_name, PROP_NAME_PREFIX)
  debug_print (2, ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> done reading and parsing props")

  #validate options
  if options.parse_file_name:
    figure_out_options(file_name, options)
  validateOptions(options)
    
  #construct expected values
  exp_props = buildExpectedValues(options)

  min_props=dict() #empty by default  - values like buffer sizes
  if options.size_data:
    min_props = calculateBufferSizes(options)

  #compare them
  return validate(exp_props, min_props, all_props);


def main(argv):
  (options, args) = parse_args(argv)
  if len(args) < 1:
    parser.error("requires exactly one xml file")
    sys.exit(1)

  #array keyed by file name
  all_results = dict()
  for file_name in args:
    all_results[file_name] = validateOneFile(file_name, options)

  output_results(all_results)
  print "=========DONE"

#######################################
if __name__ == "__main__" :
  main(sys.argv[1:])

