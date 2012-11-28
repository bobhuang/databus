#!/usr/bin/env python
'''
  Setup replication cluster with espresso master node, slave node, multi buffer relay


  Required.
  -. install mysql first
  http://apachev-md.linkedin.biz/~apachev/rpms/   sudo rpm -ivh
  restart-all-mysqld --upgrade.
  -. only works on linux as mysql setup only works there
  -. the setup-master etc should be in /usr/bin

  Note:
  -. The script will live in the databus repo as there is already some setup here
  -. It will accept a env var for ESPRESSO_REPO_DIR, if given, it will call some tools there
    o. It may include way to setup zookeeper, clustermanager, router
    o. ESPRESSO_REPO need to be compiled or the script can compile it 
  -. w/o the ESPRESSO_REPO_DIR it will follow the current test setup 
    o. reset 

  -. Will use the mysql setup in databus repo to setup multiple master and rpl-dbus slave, there will be enhanced to create user
  -. the temp directory is var


  INPUT:
   see -h

  DIRECTORY_STRUCTURE: everthing is local in the work dir which can be wiped out 

  work_dir : var/work/testname
   rpl-dbus-test
     master-15000
     slave-15000 + num_pairs
   espresso_relay_source_db_dir -> partition files for the db
   schema_registry -> copy over and generate id mapping if necessary 
   storage_node_logs -> storage node logs
     master_1
     slave_1_1 
     slave_1_2 
   

   num_pairs = num_espresso_node = num_master_node * (repl_factor + 1)
   num_mysqld = num_pairs * 2

   mysql-5.5/utils/setup-rpl-dbus-test-cluster --shutdown

2011-10-19 07:28:56,263 +48541 [FileRefreshThread_-544981017] (INFO) {CMTaskExecutor:onMessage} (CMTaskExecutor.java:222) No Messages to process

What is in the storage node property file
   # Cluster Node Id : Integer Id for the local Cluster Node
espresso.singleNode.clusterNodeId=1
   # Cluster Manager Config
espresso.singleNode.container.httpPort=12918
   # DB Schema Manager Config
espresso.singleNode.schemaRegistry.rootSchemaNamespace=schemas_registry
espresso.singleNode.schemaRegistry.mode=file
espresso.singleNode.schemaRegistry.refreshPeriodMs=1000
   # Local Indexed Store Config
espresso.singleNode.localIndexedStore.storageEngine.connectionPool.hostname=localhost
espresso.singleNode.localIndexedStore.storageEngine.connectionPool.port=3306
   # Cluster Manager Config
espresso.singleNode.clusterManagerAdmin.mode=static
espresso.singleNode.clusterManagerAdmin.zkAddress=localhost:2181
espresso.singleNode.clusterManagerAdmin.clusterName=ESPRESSO_STORAGE
espresso.singleNode.clusterManagerAdmin.instanceName=localhost_12918
espresso.singleNode.clusterManagerAdmin.configFile=integration-test/config/cluster-12345-cluster-view.json
   # Databus Client Config
espresso.singleNode.databusClient.runtime.relay(1).host=localhost
espresso.singleNode.databusClient.runtime.relay(1).port=9093
espresso.singleNode.databusClient.runtime.relay(1).sources=TEST
# Search index config
# Setting the pool size to 3000 here to work with the perf hudson set up. The MailboxDB table schemas use 10 indexes per
# partition, it has 2 indexed tables, and has 128 partitions, for a total of 2560 indexes that we need to have open.
# The index pool needs to be ~10% larger to avoid any evictions.
     espresso.singleNode.localIndexedStore.searchIndex.indexPoolSize=3000

  reset - wipe out everything before proceding
  # input variables
  # num_master_node
  # replication_factor=1
  # espresso_database_name

  # schema_registry_dir
  # generate the ppart
  # generate the schema_id_mapping or not
  #number_of_partitions  decided by the schema

  # 
  # espresso side call should accept diffrent schema_registry



'''
__author__ = "David Zhang"
__version__ = "$Revision: 0.1 $"
__date__ = "$Date: 2011/10/19 $"
import sys, os, fcntl
import pdb
import time, copy, re
from optparse import OptionParser, OptionGroup
import logging
import threading
import pexpect
from utility import *
import distutils.dir_util
import shutil, glob
        
# Global constant
EspressoPackageDir = "%s/integration-test/lib/espresso/dist/espresso-storage-node-pkg" % get_view_root()
EspressoStorageNodeJVMArgsPattern = "-Xms2048m -Xmx2048m -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseConcMarkSweepGC -XX:+HeapDumpOnOutOfMemoryError -Xloggc:%s"
EspressoStorageNodeStartPort=12930
EspressoRelayPort=11140
EspressoRelayTcpPort=11141
ComponentSeq=["mysql","router","clustermanager","master","relay","slave"]  # the sequence of things, can also start stop individually
ComponentSeqExclude=["router"]  # does not do router as default
ClusterManagerAdminPort=12925
ZookeeperConnStr="localhost:2181"

# Global varaibles
options=None

ESPRESSO_HOME=""
ESPRESSO_SCHEMA_REGISTRY_DIR=""
ESPRESSO_NUM_PARTITIONS={}   # map from db name to num partitions
ESPRESSO_TABLES={}   # map from db name to num partitions
ESPRESSO_DBS=[]   # list of dbs
MYSQL_PORTS=[]   # list of master-slave pair port
MYSQL_DIRS=[]   # mysql master-slave dir
ESPRESSO_STORAGE_NODE_IDS=[]  # master_1, slave_1_1, slave_1_2
ESPRESSO_STORAGE_NODE_PORTS=[]  # same sequence as above
ESPRESSO_STORAGE_NODE_LOG_DIR=""
COMPONENT_LIST=[]

ESPRESSO_VIEW_SCRIPT_DIR=""

############ utilities
pushd_cur_dir=None
def pushd(to_dir):
  global pushd_cur_dir
  pushd_cur_dir = os.getcwd()
  os.chdir(to_dir)

def popd():
  os.chdir(pushd_cur_dir)

########
def get_espresso_db_config_file_dir():
  return "%s/integration-test/config/espresso/json/" % get_view_root()

def get_espressoDB_config_files(source_dir=None):
  espressoDB_config_files=[]
  if not source_dir: source_dir =  get_espresso_db_config_file_dir()
  for db in ESPRESSO_DBS:
    glob_pattern = "%s/ppart__es_%s*" % (source_dir, db)
    dbg_print("glob_pattern = %s" % glob_pattern)
    #print glob_pattern
    espressoDB_config_files.extend(glob.glob("%s/ppart__es_%s_*" % (source_dir, db)))
  return espressoDB_config_files

def get_dbus_espresso_source_db_dir():
  return os.path.join(get_work_dir(),"espresso_relay_source_db_dir")

def setup_partition_mapping():
  # TODO:
  # use tool to create partition mapping ppart__es_{DBNAME} dynamically
  # For now, just copy over
  espressoDB_config_files=get_espressoDB_config_files()
   
  dbg_print("espressoDB_config_files=%s" % espressoDB_config_files)
  if not espressoDB_config_files:
    my_warning("Cannot file partition mapping for db %s" % ESPRESSO_DBS)
  dest_espressoDB_config_file_dir=get_dbus_espresso_source_db_dir()
  if os.path.exists(dest_espressoDB_config_file_dir):
    if options.reset: shutil.rmtree(dest_espressoDB_config_file_dir)
    else: 
      print "Using existing partition mapping in %s" % dest_espressoDB_config_file_dir
      return
  distutils.dir_util.mkpath(dest_espressoDB_config_file_dir, verbose=1)  
  for f in espressoDB_config_files:
    shutil.copy(f, os.path.join(dest_espressoDB_config_file_dir, os.path.basename(f)))

def setup_schema_id_mapping(dbs,dest_dir):
  schema_id_mapping_dir = os.path.join(options.schema_registry_dir,"schema_id_mapping")
  if not os.path.exists(schema_id_mapping_dir):
    # TODO:
    # call id generation  tool to create id mapping
    my_error("Cannot find schema_id_mapping dir %s" % schema_id_mapping_dir)
  dest_schema_id_mapping_dir = os.path.join(dest_dir,"schema_id_mapping")
  if os.path.exists(dest_schema_id_mapping_dir): shutil.rmtree(dest_schema_id_mapping_dir)
  #shutil.copytree(schema_id_mapping_dir,dest_schema_id_mapping_dir,True)  # copy with symlink
  #"schema_id_mapping/table","schema_id_mapping/db",
  subdirs=["db","table"]
  # somehow the mapping is empty
  #setup_schema_copy_db(dbs, subdirs, schema_id_mapping_dir,dest_schema_id_mapping_dir)

def setup_schema_copy_db(dbs, subdirs, source_root_dir, dest_root_dir):
  for dir in subdirs:
    source_dir = os.path.join(source_root_dir,dir)
    dest_dir = os.path.join(dest_root_dir,dir)
    distutils.dir_util.mkpath(dest_dir, verbose=1)  
    for db in dbs:
      source_db_dir = os.path.join(source_dir,db)
      dest_db_dir = os.path.join(dest_dir,db)
      if not os.path.exists(source_db_dir): my_error("Missing schema dir %s" % source_db_dir)
      shutil.copytree(source_db_dir,dest_db_dir,True)

def setup_schema_registry(dbs):
  schema_registry_dir = os.path.join(get_view_root(),options.schema_registry_dir)
  if not os.path.exists(schema_registry_dir):
    my_error("Cannot find schema_registry dir %s" % schema_registry_dir)
  global ESPRESSO_SCHEMA_REGISTRY_DIR
  ESPRESSO_SCHEMA_REGISTRY_DIR = os.path.join(get_work_dir(),"schemas_registry")
  if os.path.exists(ESPRESSO_SCHEMA_REGISTRY_DIR): 
    if options.reset:
      shutil.rmtree(ESPRESSO_SCHEMA_REGISTRY_DIR)
    else:
      print "Using existing schema registry directory %s" % ESPRESSO_SCHEMA_REGISTRY_DIR
      return
  distutils.dir_util.mkpath(ESPRESSO_SCHEMA_REGISTRY_DIR, verbose=1)  
  source_schemata_dir = os.path.join(schema_registry_dir,"schemata")
  dest_schemata_dir = os.path.join(ESPRESSO_SCHEMA_REGISTRY_DIR,"schemata")
  #if os.path.exists(dest_schemata_dir): shutil.rmtree(dest_schemata_dir)
  subdirs=["db","document","table"]
  #pdb.set_trace()
  new_dbs = copy.copy(dbs)
  new_dbs.append("schemata")   # also copy the schemata
  setup_schema_copy_db(dbs, subdirs, source_schemata_dir,os.path.join(ESPRESSO_SCHEMA_REGISTRY_DIR,"schemata"))
  setup_schema_id_mapping(dbs,ESPRESSO_SCHEMA_REGISTRY_DIR)

def get_number_of_partitions(dbname):
  # TODO: search schema registry and find the db name
  db_dir=os.path.join(ESPRESSO_SCHEMA_REGISTRY_DIR,"schemata/db/%s" % dbname)
  if not os.path.exists(db_dir):
    my_error("db dir does not exist: %s" % db_dir)
  db_json_files=glob.glob("%s/*.json" % db_dir)
  if not db_json_files: my_error("no db json in dir: %s" % db_dir)
  db_json_files.sort()
  db_json = json.load(open(db_json_files[-1]))
  dbg_print("db_json = %s" % db_json)
  return db_json["numBuckets"]

def  setup_espresso_log_dir():
  global ESPRESSO_STORAGE_NODE_LOG_DIR
  ESPRESSO_STORAGE_NODE_LOG_DIR = os.path.join(get_work_dir(),"storage_node_logs")
  distutils.dir_util.mkpath(ESPRESSO_STORAGE_NODE_LOG_DIR, verbose=1)  
  for id in ESPRESSO_STORAGE_NODE_IDS:
    distutils.dir_util.mkpath(os.path.join(ESPRESSO_STORAGE_NODE_LOG_DIR,id),verbose=1)  

def get_espresso_tables_for_node(storage_node_id, dbname):
  ''' return the espresso tables for node and the db, why we need this? '''
  return get_espresso_tables(dbname)

def get_espresso_tables(dbname):
  ''' return the table name give the dbname. '''
  # TODO: extract this from the mapping, now just hardcode
  #allowed_dbs=["EspressoDB"]
  schemata_dir=os.path.join(ESPRESSO_SCHEMA_REGISTRY_DIR,"schemata/document/%s" % dbname)
  if not os.path.exists(schemata_dir): my_error("schemata dir for db %s does not exist" % schemata_dir)
  pushd(schemata_dir)
  tables=glob.glob("*")
  popd()
  dbg_print("tables = %s" % tables) 
  return tables

def setup_espresso_remote_view(): 
  global ESPRESSO_VIEW_ROOT,ESPRESSO_VIEW_SCRIPT_DIR
  ESPRESSO_VIEW_ROOT=options.espresso_view_root
  ESPRESSO_VIEW_SCRIPT_DIR=os.path.join(ESPRESSO_VIEW_ROOT,"integration-test/script")

def setup_espresso(): 
  ''' create the directory under work '''
  global ESPRESSO_HOME, ESPRESSO_NUM_PARTITIONS, ESPRESSO_DBS
  ESPRESSO_DBS=options.espresso_db_names.split(",")
  dbg_print("ESPRESSO_DBS= %s" % ESPRESSO_DBS)
  setup_schema_registry(ESPRESSO_DBS)
  for dbname in ESPRESSO_DBS:
    ESPRESSO_NUM_PARTITIONS[dbname] = get_number_of_partitions(dbname)
    ESPRESSO_TABLES[dbname] = get_espresso_tables(dbname)
  ESPRESSO_HOME = os.path.join(get_view_root(),"integration-test/lib/espresso")
  setup_partition_mapping()
  #port = next_available_port("localhost",EspressoStorageNodeStartPort)
  port = EspressoStorageNodeStartPort
  if (isOpen("localhost",port)): my_warning("Port %s is used" % port)
  # setup the port
  for i in range(get_num_storage_nodes()):
    ESPRESSO_STORAGE_NODE_PORTS.append(port)
    if i < options.espresso_num_master_nodes: id = "master_%s" % i
    else: id = "slave_%s_%s" % (i % options.espresso_num_master_nodes, i / options.espresso_num_master_nodes)
    ESPRESSO_STORAGE_NODE_IDS.append(id)
    #port = next_available_port("localhost",port+1)
    port = port+2  # use +1 for management port
    if (isOpen("localhost",port)): my_warning("Port %s is used" % port)
  dbg_print("ESPRESSO_STORAGE_NODE_PORTS= %s" % ESPRESSO_STORAGE_NODE_PORTS)
  dbg_print("ESPRESSO_STORAGE_NODE_IDS= %s" % ESPRESSO_STORAGE_NODE_IDS)
  setup_espresso_log_dir()
  if options.espresso_view_root: setup_espresso_remote_view() 

def get_rpl_dbus_test_dir():
  return os.path.join(get_work_dir(),"rpl-dbus-test")

def get_num_storage_nodes():
  return  get_rpl_dbus_num_pairs()

def get_rpl_dbus_num_pairs(): # this is the number of storage node
  # this is the save as number of espresso nodes need to be up
  return options.espresso_num_master_nodes * (options.replication_factor + 1)
 
def setup_mysql_master_slave_dir():
  rpl_dbus_test_dir = get_rpl_dbus_test_dir()
  num_pairs = get_rpl_dbus_num_pairs()
  dbg_print("num_pairs = %s" % num_pairs)
  global MYSQL_PORTS, MYSQL_DIRS
  # we need to make sure we have the port since we are setting up dir based on that
  #first_port = next_available_port("localhost",15000)
  first_port = 15000 # in the script
  for i in range(num_pairs):
    master_port = first_port+i
    rpl_slave_port = first_port+i+num_pairs
    MYSQL_PORTS.append((master_port, rpl_slave_port))
    MYSQL_DIRS.append((os.path.join(rpl_dbus_test_dir,"master-%s" % master_port),os.path.join(rpl_dbus_test_dir,"slave-%s" % rpl_slave_port)))
  
def setup_mysql_master_slave():
  #mysql_util_dir = os.path.join(get_view_root(),"mysql-5.5/utils")
  mysql_util_dir = "/usr/bin"   # some how setup-master is missing, using the /usr/bin one
  rpl_dbus_test_dir = get_rpl_dbus_test_dir()
  if os.path.exists(rpl_dbus_test_dir):
    if options.reset: shutil.rmtree(rpl_dbus_test_dir)
    else:
      print "Using existing rpl_dbus_test_dir %s" % rpl_dbus_test_dir 
      return
  cur_dir = os.getcwd()
  os.chdir(mysql_util_dir)
  ret = sys_call("./setup-rpl-dbus-test-cluster --num-pairs=%s --master-dir-prefix=%s/master --slave-dir-prefix=%s/slave --script-dir=%s"
       % (get_rpl_dbus_num_pairs(), rpl_dbus_test_dir, rpl_dbus_test_dir, mysql_util_dir))
  os.chdir(cur_dir)
  dbg_print("MYSQL_DIRS=%s" % MYSQL_DIRS)
  for (master_dir, rpl_slave_dir) in MYSQL_DIRS:
    sys_call('''mysql --defaults-file=%s/my.cnf -uroot  -e "grant all on *.* to %s@localhost identified by '%s';"''' % (master_dir,"espresso","espresso"))
    sys_call('mysql --defaults-file=%s/my.cnf -uroot  -e "set global rpl_dbus_debug=1;"' % (rpl_slave_dir)) # enable debugging
    sys_call('mysql --defaults-file=%s/my.cnf -uroot  -e "set global long_query_time=0;"' % (master_dir)) # enable debugging
  print "Setup of mysql replicaiton cluster is Done"

def shutdown_mysql():
  setup_mysql_master_slave_dir()
  for (master_dir, rpl_slave_dir) in MYSQL_DIRS:
    ret = sys_call("mysqladmin --defaults-file=%s/my.cnf --shutdown_timeout=10 -uroot shutdown" % master_dir)
    if not ret: print "Error running MySQL shutdown for host with %s/my.cnf. ret = %s" % (master_dir, ret)
    ret = sys_call("mysqladmin --defaults-file=%s/my.cnf --shutdown_timeout=10 -uroot shutdown" % rpl_slave_dir)
    if not ret: print "Error running MySQL shutdown for host with %s/my.cnf. ret = %s" % (rpl_slave_dir, ret)
    sys_call("ps -ef | grep mysqld | awk '{print $2}' | xargs kill -9")
  print "End shutdown mysql"

def start_router():
  if not options.espresso_view_root:
    my_warning("espresso_view_root is not given. Cannot start router.")
    return RetCode.ERROR
  print "Espresso Router started"
  return RetCode.OK

def setup_espresso_package():
  if os.path.exists(EspressoPackageDir) : return
  cwd = os.getcwd()
  distutils.dir_util.mkpath(EspressoPackageDir, verbose=1)  
  os.chdir(EspressoPackageDir)
  storage_node_dload_url="http://hudson3.qa.linkedin.com/job/MULTIPRODUCT_ESPRESSO_CI/lastSuccessfulBuild/artifact/src/dist/espresso-storage-node-pkg/*zip*/espresso-storage-node-pkg.zip"
  local_file_name = os.path.join(EspressoPackageDir,os.path.basename(storage_node_dload_url))
  import urllib 
  open(local_file_name).write(open(urllib.urlopen(argv[0]).read())) 
  sys_call("unzip %s" % local_file_name)
  os.chdir(cwd)

def get_log_suffix():
  return "%s_%s" % (time.strftime('%y%m%d_%H%M%S'), os.getpid())

def clear_tmp_cluster_manager_dir():
  ''' both master slave using the same tmp dir will cause problems '''
  tmp_cm_dir="/tmp/espresso-schemas"
  if not os.path.exists(tmp_cm_dir): return
  def dir_cleared():
    if not os.path.exists(tmp_cm_dir): return True
    try:
      shutil.rmtree(tmp_cm_dir)
      return not os.path.exists(tmp_cm_dir)
    except:
      return False
  wait_for_condition_1(dir_cleared, timeout=240, sleep_interval=5)
 
def start_espresso_storage_nodes(node_type):
  setup_espresso_package()
  clear_tmp_cluster_manager_dir()
  class_path = ":".join(glob.glob("%s/lib/*.jar" % EspressoPackageDir))
  if not class_path: my_error("class_path is empty. Check if %s/lib" % EspressoPackageDir)
  log4j_property_file= "%s/conf/storage-node-log4j-info.properties" % EspressoPackageDir
  #log4j_property_file= "%s/conf/storage-node-log4j-debug.properties" % EspressoPackageDir
  # we still use the property file to provide some defaults
  storage_node_property_file = "%s/conf/storage-node-config.properties" % EspressoPackageDir
  storage_node_properties={
   "espresso.singleNode.clusterManagerAdmin.configFile":"%s/conf/clusterConfig.json" % EspressoPackageDir
   ,"espresso.singleNode.localIndexedStore.searchIndex.indexPoolSize":100 
  }
  if options.storage_node_use_filebased_cm:
    storage_node_properties["espresso.singleNode.clusterManagerAdmin.configFile"]="%s/conf/clusterConfig.json" % EspressoPackageDir
    storage_node_properties["espresso.singleNode.schemaRegistry.rootSchemaNamespace"]=ESPRESSO_SCHEMA_REGISTRY_DIR
  else:  # cluster
    storage_node_properties["espresso.singleNode.clusterManagerAdmin.mode"]="dynamic"
    storage_node_properties["espresso.singleNode.clusterManagerAdmin.zkAddress"]=ZookeeperConnStr
    storage_node_properties["espresso.singleNode.schemaRegistry.mode"]="zk"
    storage_node_properties["espresso.singleNode.schemaRegistry.rootSchemaNamespace"]="schemas_registry"

  if node_type == "master":
    node_list = range(options.espresso_num_master_nodes)
  else:
    node_list = range(options.espresso_num_master_nodes,get_num_storage_nodes())

  for node_index in node_list:
    id=ESPRESSO_STORAGE_NODE_IDS[node_index]
    jvm_args = EspressoStorageNodeJVMArgsPattern % os.path.join(os.path.join(ESPRESSO_STORAGE_NODE_LOG_DIR,id),"gc.log")
    if node_type == "slave": jvm_args += " -agentlib:jdwp=transport=dt_socket,suspend=n,address=localhost:8991,server=y "
    #if node_type == "slave": jvm_args += " -agentlib:jdwp=transport=dt_socket,suspend=y,address=localhost:8991,server=y "
    log_dir = os.path.join(ESPRESSO_STORAGE_NODE_LOG_DIR,id)
    log_file = os.path.join(log_dir,"espresso_%s.log" % get_log_suffix())
    lucene_index_root = os.path.join(log_dir,"lucene_index")  # need to clear the checkpoint dir
    if not os.path.exists(lucene_index_root): distutils.dir_util.mkpath(lucene_index_root)
    storage_node_properties["espresso.singleNode.localIndexedStore.storageEngine.connectionPool.port"]=MYSQL_PORTS[node_index][0]  # mysql port
    storage_node_port = ESPRESSO_STORAGE_NODE_PORTS[node_index]
    storage_node_properties["espresso.singleNode.container.httpPort"]=storage_node_port  # espresso_port
    storage_node_properties["espresso.singleNode.container.mgmtHttpPort"]=storage_node_port + 1  # management port
    storage_node_properties["espresso.singleNode.clusterManagerAdmin.instanceName"]="localhost_%s" % storage_node_port
    storage_node_properties["espresso.singleNode.localIndexedStore.searchIndex.indexRootPath"]=lucene_index_root
    if node_type == "slave": # for slave, add databus client config
      checkpoint_dir = os.path.join(log_dir,"checkpoint")  # need to clear the checkpoint dir
      if not os.path.exists(checkpoint_dir): distutils.dir_util.mkpath(checkpoint_dir)
      storage_node_properties["espresso.singleNode.replication"]="true"
      storage_node_properties["espresso.singleNode.databusClient.runtime.relay(1).host"]="localhost"
      storage_node_properties["espresso.singleNode.databusClient.runtime.relay(1).port"]=EspressoRelayPort
      storage_node_properties["espresso.singleNode.databusClient.checkpointPersistence.fileSystem.rootDirectory"]=checkpoint_dir
      storage_node_properties["espresso.singleNode.databusClient.checkpointPersistence.clearBeforeUse"]="true"
      sources = []
      for db in ESPRESSO_DBS:
        for table in get_espresso_tables_for_node(id, db): sources.append("%s.%s" % (db, table))
      storage_node_properties["espresso.singleNode.databusClient.runtime.relay(1).sources"]=".".join(sources)
    prop = ";".join(["%s=%s" % (k, storage_node_properties[k]) for k in storage_node_properties])
    sys_call('java %s -cp %s com.linkedin.espresso.storagenode.EspressoSingleNode -c "%s" -p %s -l %s 2>&1 >& %s &' 
      % (jvm_args, class_path, prop, storage_node_property_file, log4j_property_file, log_file))
    def no_messages_to_process(): 
      ret_lines = sys_pipe_call("tail %s" % log_file)
      return re.search("No Messages to process",ret_lines)
    for i in range(5):
      ret = wait_for_condition_1(no_messages_to_process, timeout=240, sleep_interval=5)
      time.sleep(1)  # wait some time so we instead get to the end
      if ret != RetCode.OK: 
        print "Time out starting Espresso Storage %s nodes in local view" % node_type
        return ret
  if node_list: print "Espresso Storage %s nodes started in local view" % node_type
  return RetCode.OK

def start_espresso(node_type):
  ''' we can start router, cm etc in the future '''
  if not options.storage_node_use_filebased_cm and not isOpen("localhost",ClusterManagerAdminPort):
    my_warning("Cluster Manager Admin port %s is not open. Storage node is NOT started" % ClusterManagerAdminPort)
    return RetCode.ERROR
  if options.espresso_view_root:
    global EspressoPackageDir
    # use local version
    #EspressoPackageDir = "%s/dist/espresso-storage-node-pkg" % options.espresso_view_root
    #print "Espresso Storage nodes started in  view %s" % options.espresso_view_root
    ret = start_espresso_storage_nodes(node_type)
    if ret != RetCode.OK: return ret
  else:
    ret = start_espresso_storage_nodes(node_type)
    if ret != RetCode.OK: return ret
  return RetCode.OK

def start_relay():
  script_dir=get_script_dir()
  test_name = get_test_name()
  espressoDB_config_files = get_espressoDB_config_files(get_dbus_espresso_source_db_dir())
  espresso_conf_dir="integration-test/config/espresso"

  # TODO: 
  # change the code accept propert file for db_config_file
  # put all the json in command line, not very good
  relay_properties={
    "databus.relay.eventBuffer.trace.filename":"%s" % os.path.join(get_work_dir(),"relay_event_trace")
    ,"databus.relay.eventBuffer.trace.appendOnly":"false"
    ,"databus.relay.container.httpPort":EspressoRelayPort
    ,"databus.relay.container.tcp.port":EspressoRelayTcpPort
    ,"databus.relay.espressoSchemas.rootSchemaNamespace": ESPRESSO_SCHEMA_REGISTRY_DIR
    ,"databus.relay.eventBuffer.maxSize":10000000
    ,"databus.relay.eventBuffer.readBufferSize":500000
    ,"databus.relay.eventBuffer.scnIndexSize":500000
  }
  prop = ";".join(["%s=%s" % (k, relay_properties[k]) for k in relay_properties])

  jvm_args = ""
  #jvm_args= '--jvm_args="-agentlib:jdwp=transport=dt_socket,suspend=y,address=localhost:8991,server=y"' 
  sys_call('%s/dbus2_driver.py -n %s -c espresso_relay -o start %s --db_config_file=%s --cmdline_props="%s" -p %s/espresso_relay.properties -l %s/espresso_relay_log4j.properties' %
    (get_script_dir(), get_test_name(), jvm_args, ",".join(espressoDB_config_files), prop, espresso_conf_dir, espresso_conf_dir))

  print "Espresso Relay started"
  return RetCode.OK

def start_master():
  return start_espresso("master")

def start_slave():
  return start_espresso("slave")

def start_mysql():
  setup_mysql_master_slave()

def start_clustermanager_setup_cluster():
  # post the schema # add cluster
  cluster_name="ESPRESSO_STORAGE"
  sys_call('''curl -d 'jsonParameters={"command":"addCluster","clusterName":"%s"}' -H "Content-Type: application/json" http://localhost:%s/clusters''' % (cluster_name, ClusterManagerAdminPort))
  # add instance
  for i in range(len(ESPRESSO_STORAGE_NODE_PORTS)):
    sys_call('''curl -d 'jsonParameters={"command":"addInstance","instanceNames":"localhost:%s"}' -H "Content-Type: application/json" http://localhost:%s/clusters/%s/instances''' % ( ESPRESSO_STORAGE_NODE_PORTS[i],ClusterManagerAdminPort, cluster_name))
  # add DB
  # we may need to slave only model and master only model
  for db in ESPRESSO_DBS:
    sys_call('''curl -d 'jsonParameters={"command":"addResourceGroup","resourceGroupName":"%s","partitions":"%s","stateModelDefRef":"MasterSlave"}' -H 'Content-Type: application/json' http://localhost:%s/clusters/%s/resourceGroups''' % (db,ESPRESSO_NUM_PARTITIONS[db], ClusterManagerAdminPort, cluster_name))
  # rebalance for one partition does not work, just upload ideal state
  #  sys_call('''curl -d 'jsonParameters={"command":"rebalance","replicas":"%s"}' -H "Content-Type: application/json" http://localhost:%s/clusters/%s/resourceGroups/%s/idealState''' % (options.replication_factor+1, ClusterManagerAdminPort,cluster_name,db)) 
    newIdealState={"id":db}
    newIdealState["simpleFields"]={"ideal_state_mode" : "AUTO", "partitions" : "%s" % ESPRESSO_NUM_PARTITIONS[db], "state_model_def_ref" : "MasterSlave"}
    newIdealState["deltaList"]=[]
    newIdealState["mapFields"]={}
    newIdealState["listFields"]={}
    for ind in range(ESPRESSO_NUM_PARTITIONS[db]):
      partition_name = "%s_%s" % (db,ind)
      if partition_name not in newIdealState["mapFields"]: 
        newIdealState["mapFields"][partition_name]={}
        newIdealState["listFields"][partition_name]=[]
      for i in range(len(ESPRESSO_STORAGE_NODE_PORTS)):
        port=ESPRESSO_STORAGE_NODE_PORTS[i]
        type=re.search("master",ESPRESSO_STORAGE_NODE_IDS[i]) and "MASTER" or "SLAVE"
        node_id="localhost_%s" % port
        newIdealState["mapFields"][partition_name][node_id]=type
        newIdealState["listFields"][partition_name].append(node_id)
    dbg_print("newIdealState=%s" % newIdealState)
    sys_call('''curl -d 'jsonParameters={"command":"alterIdealState"}&newIdealState=%s}' -H "Content-Type: application/json" http://localhost:%s/clusters/%s/resourceGroups/%s/idealState''' % (json.dumps(newIdealState), ClusterManagerAdminPort,cluster_name,db)) 

'''
echo 'jsonParameters={"command":"alterIdealState"}&newIdealState={
  "id" : "TestDB",
  "simpleFields" : {
    "ideal_state_mode" : "AUTO",
    "partitions" : "2",
    "state_model_def_ref" : "MasterSlave"
  },
  "deltaList" : [ ],
  "mapFields" : {
    "TestDB_0" : {
      "localhost_1234" : "MASTER",
      "1ocalhost_1235" : "SLAVE"
    },
    "TestDB_1" : {
      "localhost_1234" : "MASTER",
      "1ocalhost_1235" : "SLAVE"
    }
  },
  "listFields" : {
    "TestDB_0" : [ "localhost_1234", "localhost_1235" ],
    "TestDB_1" : [ "localhost_1234", "localhost_1235" ]
  }
}' > /tmp/1.txt

''' 
def start_clustermanager():
  if not options.espresso_view_root:
    my_warning("espresso_view_root is not given. Cannot start cluster manager.")
    return RetCode.ERROR
  # start zookeeper
  sys_call('%s/espresso_driver.py -c zookeeper -o start --zookeeper_server_ports=%s --zookeeper_reset --cmdline_props="tickTime=2000;initLimit=5;syncLimit=2"' % (ESPRESSO_VIEW_SCRIPT_DIR,ZookeeperConnStr))

  # start cluster manager admin
  sys_call('%s/espresso_driver.py -c clustermanager-container -o clean' % ESPRESSO_VIEW_SCRIPT_DIR)
  sys_call('%s/espresso_driver.py -c clustermanager-container -o start' % ESPRESSO_VIEW_SCRIPT_DIR)
  sys_call('%s/espresso_driver.py -c clustermanager-admin-deploy -o start' % ESPRESSO_VIEW_SCRIPT_DIR)

  # upload schema
  sys_call('''%s/espresso_driver.py -c schema-loader --cmdline_args="--schema-src-root %s --schema-dst-root schemas_registry --zk-address %s --sync-method UPDATE --upload-db-regex '.*' -l %s/perf-test/config/schema-loader-log4j.properties"''' % (ESPRESSO_VIEW_SCRIPT_DIR, ESPRESSO_SCHEMA_REGISTRY_DIR, ZookeeperConnStr, ESPRESSO_VIEW_ROOT ))

  time.sleep(2)

  start_clustermanager_setup_cluster()
  # start cluster manager
  sys_call("%s/espresso_driver.py -c clustermanager-deploy -o start" % ESPRESSO_VIEW_SCRIPT_DIR)

  # wait for rebalance to finish
  time.sleep(1)

  print "Cluster Manager started"
  return RetCode.OK

def setup_repl():
  setup_espresso()
  setup_mysql_master_slave_dir()
  for c in ComponentSeq:
    if c in COMPONENT_LIST: globals()["start_%s" % c]()
  return RetCode.OK

def shutdown_master():
  shutdown_espresso()

def shutdown_slave():
  shutdown_espresso()

def shutdown_clustermanager():
  if not options.espresso_view_root:
    my_warning("espresso_view_root is not given. Cannot shutdown cluster manager.")
    return RetCode.ERROR
  sys_call("%s/espresso_driver.py -c clustermanager-container -o stop" % ESPRESSO_VIEW_SCRIPT_DIR)
  sys_call("%s/espresso_driver.py -c zookeeper -o stop" % ESPRESSO_VIEW_SCRIPT_DIR)
  print "Cluster Manager is shutdown"
  return RetCode.OK

def shutdown_router():
  pass

def shutdown_espresso():
  sys_call("jps | grep EspressoSingleNode | cut -d ' ' -f 1 | xargs kill -9")

def shutdown_relay():
   sys_call("%s/dbus2_driver.py -n %s -c espresso_relay -o stop" % (get_cwd(), options.testname))

def shutdown_component(input_clist=COMPONENT_LIST):
  clist = copy.copy(ComponentSeq)
  clist.reverse()
  if options.espresso_view_root: setup_espresso_remote_view()
  for c in clist:
    if c in COMPONENT_LIST: globals()["shutdown_%s" % c]()

def shutdown_all():
  shutdown_component(ComponentSeq)

def setup_component_list():
  global COMPONENT_LIST
  option_clist = [c for c in options.component.split(",") if c] # get rid of null
  clist = options.component and option_clist or copy.copy(ComponentSeq)
  for c in clist:
    if c not in ComponentSeq:
      my_error("Component '%s' is not in %s" % (c, ComponentSeq))
  for c in clist:
    if c in ComponentSeqExclude and c not in option_clist: clist.remove(c)  # do not remove if given in cmd line
  # order
  COMPONENT_LIST = [c for c in ComponentSeq if c in clist]
  dbg_print("COMPONENT_LIST = %s" % COMPONENT_LIST)
   
def main(argv):
  global options
  parser = OptionParser(usage="usage: %prog [options]")
  parser.add_option("-n", "--testname", action="store", dest="testname", default=None, help="A test name identifier")
  parser.add_option("-m", "--espresso_num_master_nodes", action="store", type="int", dest="espresso_num_master_nodes", default=1,
                     help="Number of master nodes. [default: %default]")
  parser.add_option("-r","--replication_factor", action="store", type="int", dest="replication_factor", default=1,
                     help="How many espresso slave nodes each master have. Zero means only setup master [default: %default]")
  parser.add_option("-s", "--schema_registry_dir", action="store", dest="schema_registry_dir", default="schemas_registry/espresso_test",
                     help="Schema registry base dir. We copy over to local. [default: %default]")
  parser.add_option("", "--espresso_db_names", action="store", dest="espresso_db_names", default="EspressoDB",
                     help="The db . We copy over to local. [default: %default]")
  parser.add_option("", "--espresso_view_root", action="store", dest="espresso_view_root", default=None,
                     help="Espresso checkout root. Used to start stuff from espresso side. If None, use local stuff. [default: %default]")
  parser.add_option("-c", "--component", action="store", dest="component", default="",
                       help="common separated list of component to start. %s" % ComponentSeq)
  parser.add_option("", "--storage_node_use_filebased_cm", action="store_true", dest="storage_node_use_filebased_cm", default = False, help="Start storage node using filed based cm. By default we use dynamic cm and zookeeper.[default: %default]")
  parser.add_option("", "--reset", action="store_true", dest="reset", default = False, help="reset everything if set [default: %default]")
  parser.add_option("", "--shutdown", action="store_true", dest="shutdown", default = False, help="shutdown everything if set [default: %default]")

  debug_group = OptionGroup(parser, "Debug options", "")
  debug_group.add_option("-d", "--debug", action="store_true", dest="debug", default = False,
                     help="debug mode")
  debug_group.add_option("", "--sys_call_debug", action="store_true", dest="enable_sys_call_debug", default = False,
                     help="debug sys call")
  parser.add_option_group(debug_group)

  (options, args) = parser.parse_args()
  set_debug(options.debug)
  set_sys_call_debug(options.enable_sys_call_debug)
  dbg_print("options = %s  args = %s" % (options, args))

  setup_component_list()
 
  if (not options.testname):
    options.testname = "TEST_NAME" in os.environ and os.environ["TEST_NAME"] or "default"
  os.environ["TEST_NAME"]= options.testname;
  
  if (not "WORK_SUB_DIR" in os.environ): 
      os.environ["WORK_SUB_DIR"] = "log"
  if (not "LOG_SUB_DIR" in os.environ):
      os.environ["LOG_SUB_DIR"] = "log"
  setup_work_dir()

  sys_call_debug_begin()
  if options.reset: 
    shutdown_all()
  if options.shutdown:
    shutdown_component()
    sys.exit(RetCode.OK)
  ret = setup_repl()
  sys_call_debug_end()
  sys.exit(ret)
  
if __name__ == "__main__":
  main(sys.argv[1:])


