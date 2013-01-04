#!/usr/bin/env python
'''
This module replaces the earlier method of launching a subcomponent using an ant target inside the run directory.
Earlier - dbus2_driver.py invoked an ant target, and therefore had to translate java command line paramters into 
a syntax ant could map out as java or jvm paramters.
'''
import sys;
import os;
import os.path;
from optparse import OptionParser
import pdb;

instrumented_build_jars = [os.path.join(os.getcwd(),"build/cobertura/instrumented_classes")]

view_root=os.getcwd()

env = os.getenv("CODE_COVERAGE")

# Invoke Java program
def invokeProgram(cp, jvm_arg_line, main_class, java_arg_line):
  global env
  if env is not None:
    print "CC=" + env
    cmd = "java -cp " + cp + " " + jvm_arg_line  + " -Dnet.sourceforge.cobertura.datafile=./build/cobertura/cobertura_merged.ser " + main_class + " " + java_arg_line;
  else:
    cmd = "java -cp " + cp + " " + jvm_arg_line + main_class + " " + java_arg_line
  print "jvm args= " + jvm_arg_line
  print "java args=" + java_arg_line
  print "main_class=" + main_class
  print "cp=" + cp
  sys.stdout.flush()
  os.system(cmd)

def cp_from_runpy_path(runpy_path):
  """ Returns the builddir path given a path to the run.py for a module.
    This is to find the classpath file in the build dir for a given module.
    :param str runpy_path:
    :rtype: str
  """
  # runpy_path is something like:
  #   databus2-linkedin-relay/databus2-linkedin-relay-run/run.py
  #   databus3-relay/databus3-relay-cmdline-pkg/script/run.py
  #

  if runpy_path.startswith("./"):
    runpy_path = runpy_path[2:]
  parts =  runpy_path.split("/", 2)
  cpfile = os.path.join("build", parts[1], parts[1] + ".classpath")
  if not os.path.exists(cpfile):
    raise Exception("Classpath file "+cpfile+" not found for module "+parts[1]+". Has this been built?")
  with open(cpfile, "r") as cpfh:
    return cpfh.readline().strip()

def main_base(argv, script_dir, module_name):
  print "\n --Invoking run_base.py target -- \n"
  jvm_arg_line = ""
  java_arg_line = ""
  # Parse command line
  parser = OptionParser()


  ## Command line args
  parser.add_option("--Dbootstrap.host", dest="bootstrap_host") 
  parser.add_option("--Dbootstrap.port", dest="bootstrap_port") 
  parser.add_option("--Dcluster.name", dest="cluster_name") 
  parser.add_option("--Dhttp.port", dest="http_port")
  parser.add_option("--Djmx.service.port", dest="jmx_service_port")
  parser.add_option("--Dconfig.file", dest="config_file") 
  parser.add_option("--Dconfig.dir", dest="config_dir")
  parser.add_option("--Ddump.file", dest="dump_file") 
  parser.add_option("--Dvalue.dump.file", dest="value_dump_file") 
  parser.add_option("--Dlog4j.file", dest="log4j_file")
  parser.add_option("--Djvm.direct.memory.size", dest="jvm_direct_memory_size") 
  parser.add_option("--Djvm.max.heap.size", dest="jvm_max_heap_size", default="2g")
  parser.add_option("--Djvm.min.heap.size", dest="jvm_min_heap_size", default="100m")
  parser.add_option("--Djvm.gc.log", dest="jvm_gc_log")
  parser.add_option("--Djvm.args", dest="jvm_args") 
  parser.add_option("--Djvm.debugport", dest="jvm_debugport")
  parser.add_option("--Ddb.relay.config", dest="db_relay_config") 
  parser.add_option("--Dcmdline.props", dest="cmdline_props") 
  parser.add_option("--Dfilter.conf.file", dest="filter_conf_file") 
  parser.add_option("--Dcheckpoint.dir", dest="checkpoint_dir") 
  parser.add_option("--Drelay.host", dest="relay_host") 
  parser.add_option("--Drelay.port", dest="relay_port") 
  parser.add_option("--Dconsumer.event.pattern", dest="consumer_event_pattern") 
  parser.add_option("--Dseeder.db.relay.config", dest="seeder_db_relay_config")
  parser.add_option("--Denable_jvm_debug", action="store_true", dest="enable_jvm_debug", default=False)

  # Identify test type
  parser.add_option("--Dtest")

  # Parser arguments
  (options, args) = parser.parse_args()

  #<TODO> Cleanup
  options.config_dir = os.path.join(script_dir, "config")
  #<TODO> Cleanup
  if ( options.jvm_debugport == None ):
    options.jvm_debugport = 8998

  if (options.enable_jvm_debug):
    jvm_arg_line = " -Xrunjdwp:transport=dt_socket,suspend=n,address=localhost:" + str(options.jvm_debugport) + ",server=y "

  # figure out the run target 
  #bootstrap-client targets
  if module_name == "bootstrap-client":
    if options.Dtest == "run":
      main_class = "com.linkedin.databus.client.bootstrap.DatabusBootstrapDummyConsumer"
    elif options.Dtest == "run-integrate":
      main_class = "com.linkedin.databus.client.bootstrap.IntegratedDummyDatabusConsumer"
    else:
      print "Unknown target for module: " + module_name + "-Erroring out"
      sys.exit(1)
  #bootstrap-producer targets
  elif module_name == "bootstrap-producer":
    if options.Dtest == "runproducer":
      main_class="com.linkedin.databus.bootstrap.producer.DatabusBootstrapProducer";
      if options.config_file == None:
        options.config_file = os.path.join(options.config_dir, "bootstrap-service-config.properties")
    else:
      print "Unknown target for module: " + module_name + "-Erroring out"
      sys.exit(1)
  #bootsrap-server targets
  elif module_name == "bootstrap-server":
    if options.Dtest == "run":
      main_class = "com.linkedin.databus.bootstrap.server.BootstrapHttpServer";
    elif options.Dtest == "run-debug": 
      main_class = "com.linkedin.databus.bootstrap.server.BootstrapHttpServer";
      java_arg_line = jvm_arg_line + " -d" 
    elif options.Dtest == "run-log-file":
      main_class = "com.linkedin.databus.bootstrap.server.BootstrapHttpServer";
      if options.log4j_file == None:
        options.log4j_file = os.path.join(options.config_dir, "relay-log4j2file.properties")
    elif options.Dtest == "run-debug-file": 
      main_class = "com.linkedin.databus.bootstrap.server.BootstrapHttpServer";
      if options.log4j_file == None:
        options.log4j_file = os.path.join(options.config_dir,"relay-log4j2file.properties")
      java_arg_line = jvm_arg_line + " -d"
    elif options.Dtest == "run-trace-file":
      main_class = "com.linkedin.databus.bootstrap.server.BootstrapHttpServer";
      if options.config_file == None:
        options.config_file = os.path.join(options.config_dir,"relay-trace2file.properties")
    else:
      print "Unknown target for module: " + module_name + "-Erroring out"
      sys.exit(1)
  #bootsrap-server targets
  elif module_name == "fault-bootstrap-server":
    if options.Dtest == "run":
      main_class = "com.linkedin.databus.bootstrap.test.server.fault.FaultInjectionBootstrapHttpServer";
    elif options.Dtest == "run-debug": 
      main_class = "com.linkedin.databus.bootstrap.test.server.fault.FaultInjectionBootstrapHttpServer";
      java_arg_line = jvm_arg_line + " -d" 
    elif options.Dtest == "run-log-file":
      main_class = "com.linkedin.databus.bootstrap.test.server.fault.FaultInjectionBootstrapHttpServer";
      if options.log4j_file == None:
        options.log4j_file = os.path.join(options.config_dir, "relay-log4j2file.properties")
    elif options.Dtest == "run-debug-file": 
      main_class = "com.linkedin.databus.bootstrap.test.server.fault.FaultInjectionBootstrapHttpServer";
      if options.log4j_file == None:
        options.log4j_file = os.path.join(options.config_dir,"relay-log4j2file.properties")
      java_arg_line = jvm_arg_line + " -d"
    elif options.Dtest == "run-trace-file":
      main_class = "com.linkedin.databus.bootstrap.test.server.fault.FaultInjectionBootstrapHttpServer";
      if options.config_file == None:
        options.config_file = os.path.join(options.config_dir,"relay-trace2file.properties")
    else:
      print "Unknown target for module: " + module_name + "-Erroring out"
      sys.exit(1)
  #bootstrap-utils targets
  elif module_name == "bootstrap-utils":
    options.config_file = os.path.join(options.config_dir, "bootstrap-service-config.properties")
    if options.Dtest == "runseeder":
      main_class = "com.linkedin.databus.bootstrap.utils.BootstrapSeederMain";
    elif options.Dtest == "runcleaner":
      main_class = "com.linkedin.databus.bootstrap.utils.BootstrapDBCleanerMain";
    elif options.Dtest == "runreader": 
      main_class = "com.linkedin.databus.bootstrap.utils.BootstrapDBReader";
    elif options.Dtest == "runauditor": 
      main_class = "com.linkedin.databus.bootstrap.utils.BootstrapAuditMain";
      java_arg_line = jvm_arg_line + " -d" 
    else:
      print "Unknown target for module: " + module_name + "-Erroring out"
      sys.exit(1)
  #profile-client
  elif module_name == "profile-client":  
    if options.Dtest == "run":
      main_class="com.linkedin.databus.client.member2.SimpleProfileConsumer";
    elif options.Dtest == "run-bizfollow": 
      main_class="com.linkedin.databus.client.bizfollow.SimpleBizfollowConsumer";
    elif options.Dtest == "run-conn": 
      main_class="com.linkedin.databus.client.conns.SimpleConnectionsConsumer";
    elif options.Dtest == "run-liar-lb": 
      main_class="com.linkedin.databus.client.liar.LiarLoadBalancedClient";
    elif options.Dtest == "run-liar": 
      main_class="com.linkedin.databus.client.liar.SimpleLiarConsumer";
    elif options.Dtest == "run-debug": 
      main_class="com.linkedin.databus.client.member2.SimpleProfileConsumer";
      java_arg_line = java_arg_line + " -d " + "-agentlib:jdwp=transport=dt_socket,suspend=n,address=localhost:8999,server=y " ;
    elif options.Dtest == "run-profiling": 
      main_class="com.linkedin.databus.client.member2.SimpleProfileConsumer";
      if options.log4j_file == None:
        options.log4j_file  = os.path.join(script_dir,"..","..","relay-log4j2file.properties")
    else:
      print "Unknown target for module: " + module_name + "-Erroring out"
      sys.exit(1)
  #profile-relay targets
  elif module_name == "profile-relay":
    if options.Dtest == "run":
      main_class = "com.linkedin.databus.relay.member2.Member2RelayServer"
    elif options.Dtest == "run-bizfollow": 
      main_class = "com.linkedin.databus.relay.bizfollow.BizFollowRelayServer"
    elif options.Dtest == "run-conn": 
      main_class = "com.linkedin.databus.relay.conns.ConnectionsRelayServer"
    elif options.Dtest == "run-db-relay": 
      main_class = "com.linkedin.databus.relay.test.DBTestRelayServer"
    elif options.Dtest == "run-liar": 
      main_class = "com.linkedin.databus.relay.liar.LiarRelayServer"
    elif options.Dtest == "run-fault-relay": 
      main_class = "com.linkedin.databus.relay.fault.FaultInjectionHttpRelay"
    elif options.Dtest == "run-small-buffer": 
      main_class = "com.linkedin.databus.relay.member2.Member2RelayServer"
      if options.config_file == None:
        options.config_file = os.path.join(options.config_dir,"m2-relay-config-small.properties")
    elif options.Dtest == "run-debug": 
      main_class = "com.linkedin.databus.relay.member2.Member2RelayServer";
      java_arg_line = java_arg_line + " -d";
      if options.config_file == None:
        options.config_file = os.path.join(options.config_dir,"relay-config-template.properties")
    elif options.Dtest == "run-profiling": 
      main_class = "com.linkedin.databus.relay.member2.Member2RelayServer"
      jvm_arg_line = jvm_arg_line + " -agentpath:/Applications/YourKit Java Profiler 7.5.6.app/bin/mac/libyjpagent.jnilib=port=12007 "
      if options.config_file == None:
        options.config_file = os.path.join(options.config_dir,"relay-config-template.properties")
    elif options.Dtest == "run-log-file": 
      main_class = "com.linkedin.databus.relay.member2.Member2RelayServer"
      if options.log4j_file == None:
        options.log4j_file = os.path.join(options.config_dir,"relay-log4j2file.properties")
      if options.config_file == None:
        options.config_file = os.path.join(options.config_dir,"relay-config-template.properties")
    elif options.Dtest == "run-debug-file": 
      main_class = "com.linkedin.databus.relay.member2.Member2RelayServer";
      java_arg_line = java_arg_line + " -d";
      if options.log4j_file == None:
        options.log4j_file = os.path.join(options.config_dir,"relay-log4j2file.properties")
      if options.config_file == None:
        options.config_file = os.path.join(options.config_dir,"relay-config-template.properties")
    elif options.Dtest == "run-trace-file": 
      main_class = "com.linkedin.databus.relay.member2.Member2RelayServer"
      jvm_arg_line = jvm_arg_line + " -aoserverentlit:jdwp=transport=dt_socket,suspend=n,address=localhost:" + options.jvm_debugport + ",server=y "
      if options.log4j_file == None:
        options.log4j_file = os.path.join(options.config_dir,"relay-trace2file.properties")
      if options.config_file == None:
        options.config_file = os.path.join(options.config_dir,"relay-config-template.properties")
    elif (options.Dtest == "run-large-buffer"): 
      main_class = "com.linkedin.databus.relay.member2.Member2RelayServer";
      java_arg_line = java_arg_line + " -d";
      if options.config_file == None:
        options.config_file = os.path.join(options.config_dir,"relay-largebuffer.properties")
      else:
        print "Unknown target for module: " + module_name + "-Erroring out"
        sys.exit(1)
  #relay targets
  elif module_name == "relay": 
    if options.Dtest == "run":
      main_class = "com.linkedin.databus.container.netty.HttpRelay";
    elif options.Dtest == "run-db-relay":
      main_class = "com.linkedin.databus2.relay.DatabusRelayMain"
    elif options.Dtest == "run-debug": 
      main_class = "com.linkedin.databus.container.netty.HttpRelay";
      jvm_arg_line = jvm_arg_line + " -aoserverentlit:jdwp=transport=dt_socket,suspend=n,address=localhost:" + options.jvm_debugport + ",server=y "
      java_arg_line = jvm_arg_line + " -d" 
    elif options.Dtest == "run-log-file":
      main_class = "com.linkedin.databus.container.netty.HttpRelay";
      if options.log4j_file == None:
        options.log4j_file = os.path.join(options.config_dir, "relay-log4j2file.properties")
    elif options.Dtest == "run-debug-file": 
      main_class = "com.linkedin.databus.container.netty.HttpRelay";
      if options.log4j_file == None:
        options.log4j_file = os.path.join(options.config_dir, "relay-log4j2file.properties")
      java_arg_line = jvm_arg_line + " -d"
    elif options.Dtest == "run-trace-file":
      main_class = "com.linkedin.databus.container.netty.HttpRelay";
      if options.config_file == None:
        options.config_file = os.path.join(options.config_dir, "relay-trace2file.properties")
    elif options.Dtest == "run-debug-customconf": 
       main_class = "com.linkedin.databus.container.netty.HttpRelay";
       if options.config_file == None:
         options.config_file = os.path.join(options.config_dir,"relay-config.properties")
    else:
      print "Unknown target for module: " + module_name + "-Erroring out"
      sys.exit(1)
#espresso relay targets
  elif module_name == "espresso_relay":
    if options.Dtest == "run":
      main_class = "com.linkedin.databus3.espresso.EspressoRelay"
    else:
      print "Unknown target for module: " + module_name + "-Erroring out"
      sys.exit(1)
#espresso client targets
  elif module_name == "espresso_client":
    if options.Dtest == "run":
      main_class = "com.linkedin.databus3.espresso.client.test.EspressoTestDatabusClient"
    else:
      print "Unknown target for module: " + module_name + "-Erroring out"
      sys.exit(1)
  #something went wrong
  else:
    print "Unknown target for module: " + module_name + "-Erroring out"
    sys.exit(1)
  #get the rest of the options
   
  if options.cluster_name != None:
    java_arg_line = java_arg_line + " --cluster_name=" + options.cluster_name + " "
  if options.bootstrap_host != None:
    java_arg_line = java_arg_line + " --bootstrap_host=" + options.bootstrap_host + " "
  if options.bootstrap_port != None:
    java_arg_line = java_arg_line + " --bootstrap_port=" + options.bootstrap_port + " "

  if options.http_port != None:
    java_arg_line = java_arg_line + " --http_port=" + options.http_port + " "

  if options.jmx_service_port != None:
    java_arg_line = java_arg_line + " --jmx_service_port=" + options.jmx_service_port + " "

  if options.filter_conf_file != None:
    java_arg_line = java_arg_line + "--filter_conf_file=" + options.filter_conf_file + " "

  if options.checkpoint_dir != None:
    java_arg_line = java_arg_line + "--checkpoint_dir=" + options.checkpoint_dir + " "

  if options.relay_host != None:
    java_arg_line = java_arg_line + "--relay_host=" + options.relay_host + " "

  if options.relay_port != None:
    java_arg_line = java_arg_line + "--relay_port=" + options.relay_port + " "

  if options.value_dump_file != None:
    java_arg_line = java_arg_line + "--value_file=" + options.value_dump_file + " "

  if options.consumer_event_pattern != None:
    java_arg_line = java_arg_line + "--event_pattern=" + options.consumer_event_pattern + " "

  if options.db_relay_config != None:
    java_arg_line = java_arg_line + " --db_relay_config=" + options.db_relay_config + " "
  
  if options.dump_file != None:
    java_arg_line = java_arg_line + " -f " + options.dump_file + " "

  if options.log4j_file != None:
    java_arg_line = java_arg_line + " -l " + options.log4j_file + " "

  if options.config_file != None:
    java_arg_line = java_arg_line + " -p " + options.config_file + " "

  if options.cmdline_props != None:
    java_arg_line = java_arg_line + " -c '" + options.cmdline_props + "' "


  if options.seeder_db_relay_config != None:
    java_arg_line = java_arg_line + " -c " + options.seeder_db_relay_config + " "
    
# JVM arguments
  jvm_arg_line = jvm_arg_line + " -d64 -ea "

  if options.jvm_direct_memory_size != None:
    jvm_arg_line = jvm_arg_line + "-XX:MaxDirectMemorySize=" + options.jvm_direct_memory_size + " "

  if options.jvm_max_heap_size != None:
    jvm_arg_line = jvm_arg_line + "-Xmx" + options.jvm_max_heap_size + " "
  if options.jvm_min_heap_size != None:
    jvm_arg_line = jvm_arg_line + "-Xms" + options.jvm_min_heap_size + " "

  if options.jvm_gc_log != None:
    jvm_arg_line = jvm_arg_line + "-Xloggc:" + options.jvm_gc_log + " "

  if options.jvm_args != None:
    jvm_arg_line = jvm_arg_line + options.jvm_args + " "
  
  # read classpath in from the build/<module>/<module>.classpath file
  cp_path = cp_from_runpy_path(sys.argv[0])

  if env is not None:
      print "CC=" + env
      cp_path+=":".join([os.path.join(view_root,x) for x in instrumented_build_jars])
  # Launch java program
  invokeProgram(cp_path, jvm_arg_line, main_class, java_arg_line)

