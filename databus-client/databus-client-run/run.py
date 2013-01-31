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
'''
This module replaces the earlier method of launching a subcomponent using an ant target inside the run directory.
Earlier - dbus2_driver.py invoked an ant target, and therefore had to translate java command line paramters into 
a syntax ant could map out as java or jvm paramters.
'''

import sys;

import os;
import os.path;
from optparse import OptionParser

# Setup parameters
script_dir = os.getcwd()
lib_dir    = os.path.join(script_dir, "..", "impl", "lib")
logs_dir   = os.path.join(script_dir, "..", "impl", "logs")

# JVM arguments
jvm_direct_memory_size="1024m "
jvm_direct_memory="-XX:MaxDirectMemorySize=" + jvm_direct_memory_size

jvm_min_heap_size="768m "
jvm_min_heap="-Xms" + jvm_min_heap_size

jvm_max_heap_size="1524m "
jvm_max_heap="-Xmx"+jvm_max_heap_size

jvm_gc_log= os.path.join(logs_dir, "gc.log")
jvm_gc_log_option="-Xloggc:" + jvm_gc_log + " "

# Set Classpath
def setClassPath():
  try:
    cp = "."
    jarFiles = os.listdir(lib_dir)
    for f in lib_dir: 
      if os.path.isfile(f): 
        if os.path.splitext(f)[1] in "jar":
          cp = cp + ":" + f
    print "Classpath is set to: " + cp
  except:
    print "Error in enumerating jar files from lib directory"
  return cp

# Invoke Java program
def invokeProgram(cp, jvm_arg_line, main_class, java_arg_line):
  cmd = "java -cp " + cp + " " + jvm_arg_line  + " " + main_class + " " + java_arg_line
  os.system(cmd)

def main(argv):
  # Construct parameters
  cp           = setClassPath()
  jvm_arg_line = " -d64 -ea " + jvm_direct_memory + jvm_min_heap + jvm_max_heap + jvm_gc_log_option

  # Parse command line
  parser = OptionParser()
  
  ## Command line args
  parser.add_option("--Dbootstrap.port", dest="bootstrap_port") 
  parser.add_option("--Dhttp.port", dest="http_port")
  parser.add_option("--Djmx.service.port", dest="jmx_service_port")
  parser.add_option("--Dconfig.file", dest="config_file") 
  parser.add_option("--Ddump.file", dest="dump_file") 
  parser.add_option("--Dvalue.dump.file", dest="value_dump_file") 
  parser.add_option("--Dlog4j.file", dest="log4j_file")
  parser.add_option("--Djvm.direct.memory.size", dest="jvm_direct_memory_size") 
  parser.add_option("--Djvm.max.heap.size", dest="jvm_max_heap_size")
  parser.add_option("--Djvm.gc.log", dest="jvm_gc_log")
  parser.add_option("--Djvm.args", dest="jvm_args") 
  parser.add_option("--Ddb.relay.config", dest="db_relay_config") 
  parser.add_option("--Dcmdline.props", dest="cmdline_props") 
  parser.add_option("--Dfilter.conf.file", dest="filter_conf_file") 

  # Identify test type
  parser.add_option("--Dtest")

  # Parser arguments
  (options, args) = parser.parse_args()

  # Arguments to the Java executable
  java_arg_line = options.config_file + " " + options.cmdline_props + " " + options.log4j_file

  if (options.Dtest == "run"): 
    main_class="com.linkedin.databus.client.bootstrap.DatabusBootstrapDummyConsumer";
  else: # "run-integrate"
    main_class="com.linkedin.databus.client.bootstrap.IntegratedDummyDatabusConsumer"; 

  print options.Dtest
  print main_class;

  # Launch java program
  invokeProgram(cp, jvm_arg_line, main_class, java_arg_line)

if __name__=="__main__":
  main(sys.argv[1:])

