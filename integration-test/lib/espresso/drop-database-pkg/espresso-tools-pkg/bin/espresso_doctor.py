#!/usr/bin/env python
'''
  Use to triage espresso issues

  input "helix_admin_host helix_admin_port zk_conn_str cluster_name" 
  check_partition_status
    liveinstances, # master, slave partitions
    good or bad
    bad, command to run
  check_replication
    good or bad
  repair --dry_run

 http://eat1-app113.corp:12929/clusters/ESPRESSO_DEV_SANDBOX_3/resourceGroups/ucp/externalView
   
'''
__author__ = "David Zhang"
__version__ = "$Revision: 0.1 $"
__date__ = "$Date: 2012/04/19 $"
import sys, os
import pdb
import re
from optparse import OptionParser, OptionGroup
from utility import *
import httplib
import pprint
import urllib2
        
# Global constant #PossibleCommands=["BackupNow","BackupNext","Restore","ArchiveNow","ArchiveNext"]
PossiblePartitionState=["ERROR","OFFLINE","MASTER","SLAVE"]

# Global varaibles
options=None
helix_url_clusters=None    #  /clusters  all the cluster
helix_url_cluster=None     #  the cluster we care

ExternalViews={}
IdealStates={}
UseZooKeeperForExternalView=False
# generated
DatabasePartitionMapping={}  #  {dbname: [list of partitions] }
StateCountDictEV={}  # {[dbname][instance_name]: MASTER, count, ERROR, count }
StateCountDictIS={}  # {[dbname][instance_name]: MASTER, count, ERROR, count }
InstancePartitionMapping={}  # instance to partition mapping db_name {set([I1, I2, I3]) -> {db:partitions}, external view

# status
PartitionMismatch=[]   #  dbname:partition, list of missing partition in external view
PartitionNoMaster=[]   #  dbname,partition
PartitionWrongNumberofSlaves=[]   #  dbname,partition
PartitionErrorState=[]   #  dbname,partition
InstanceErrorGroups=[]   #  The group of instances need to restart
ReplicationFactor=None
CLUSTER_MANAGER_LOG_DIR=this_file_dirname
JarLibPath=os.path.abspath(os.path.join(this_file_dirname,"../lib"))
ConfigPath=os.path.abspath(os.path.join(this_file_dirname,"../config"))
ESPRESSO_SCHEMA_REGISTRY_DIR="schemas_registry"

ESPRESSO_NUM_PARTITIONS={}   # map from db name to num partitions
ESPRESSO_TABLES={}   # map from db name to num partitions
ESPRESSO_DBS=[]   # list of dbs
MASTER_MYSQL_PORTS=[]   # list of master mysql ports
MASTER_INDEX_MYSQL_PORTS=[]   #list of master index mysql ports
MASTER_INDEX_FILE_PATH=[]    #list of file-based mysql index paths
ESPRESSO_STORAGE_NODE_ADDRS=[]  # same sequence as above, hostname:port
ROW_COUNT_TABLES=[]    # tables to check row count

DR_MASTER_MYSQL_PORTS=[]   # list of master mysql ports
DR_MASTER_INDEX_MYSQL_PORTS=[]   #list of master index mysql ports
DR_MASTER_INDEX_FILE_PATH=[]    #list of file-based mysql index paths
DR_ESPRESSO_STORAGE_NODE_ADDRS=[]  # same sequence as above, hostname:port

#==============
# Utilities

def ask_to_continue(msg):
  ''' print the message. Accept STOP to quit. SKIP to skip the step. Enter to continue '''
  new_msg = "%s (Enter to continue, SKIP to skip the step, STOP to quit all):" % msg
  input = raw_input(new_msg)
  if input == "SKIP": return False
  if input == "STOP": my_exit(RetCode.ERROR)
  return True

#==============
def get_log_name(component, oper):
  return "%s_%s_%s_%s.log" % (component, oper, time.strftime('%y%m%d_%H%M%S'), os.getpid())

def get_cm_log_name(component, oper):
  return os.path.join(CLUSTER_MANAGER_LOG_DIR, get_log_name(component,oper))

EXTERNAL_VIEW={}
def reset_external_view():
  global EXTERNAL_VIEW
  EXTERNAL_VIEW={}
def get_external_view(cluster_name, db_name):
  global EXTERNAL_VIEW
  if cluster_name in EXTERNAL_VIEW and db_name in EXTERNAL_VIEW[cluster_name]: return EXTERNAL_VIEW[cluster_name][db_name]
  if UseZooKeeperForExternalView:
    jsons = get_json_or_list_from_zookeeper("/%s/EXTERNALVIEW/%s" % (cluster_name, db_name), getList=False)
    external_view_dict = json.loads(jsons)["mapFields"]
  else:
    url = "%s/%s/externalView" % (helix_url_cluster, db_name)
    ret = urllib.urlopen(url).read()
    external_view_dict = json.loads(ret)["mapFields"]
  #dbg_print("external_view_dict = %s" % external_view_dict)
  if cluster_name not in EXTERNAL_VIEW:
    EXTERNAL_VIEW[cluster_name] = {}
  EXTERNAL_VIEW[cluster_name][db_name] = external_view_dict
  return external_view_dict

IDEAL_STATES={}
def get_ideal_state(cluster_name, db_name):
  global IDEAL_STATES
  if db_name in IDEAL_STATES: return IDEAL_STATES[db_name]
  if UseZooKeeperForExternalView:
    jsons = get_json_or_list_from_zookeeper("/%s/IDEALSTATES/%s" % (cluster_name, db_name), getList=False)
    ideal_state_dict = json.loads(jsons)["mapFields"]
  else:
    url = "%s/%s/idealState" % (helix_url_cluster, db_name)
    ret = urllib.urlopen(url).read()
    ideal_state_dict = json.loads(ret)["mapFields"]
    #return json.loads(ret,"latin-1")["mapFields"]
  dbg_print("ideal_state_dict = %s" % ideal_state_dict)
  IDEAL_STATES[db_name] = ideal_state_dict
  return ideal_state_dict

def get_zk_list(cluster_name, zkpath):
  zk_list = get_json_or_list_from_zookeeper("/%s/%s" % (cluster_name, zkpath), getList=True)
  ret_list = convert_zookeeper_list(zk_list)
  return ret_list

def get_live_instances(cluster_name):
  live_instances = get_zk_list(cluster_name, "LIVEINSTANCES")
  dbg_print("live instances = %s" % live_instances)
  return live_instances

def get_db_names(cluster_name):
  if UseZooKeeperForExternalView:
    db_list = get_json_or_list_from_zookeeper("/%s/EXTERNALVIEW" % (cluster_name), getList=True)
    dbs = convert_zookeeper_list(db_list)
  else:
    url = helix_url_cluster
    dbg_print("url = %s" % url)
    dbs = urllib.urlopen(url).read()
    #dbg_print("ret = %s" % ret)
    ret = json.loads(ret)["listFields"]["ResourceGroups"]
    #ret = json.loads(ret,"latin-1")["listFields"]["ResourceGroups"]
  dbg_print("dbs = %s" % dbs)
  return [x for x in dbs if x!="PARTICIPANT_LEADER_EspressoRebalancer" and x!="schemata"]

def get_number_of_partitions(dbname):
  ''' always use zookeeper '''
  jsons= get_json_or_list_from_zookeeper("/%s/schemata/db/%s/1" % (ESPRESSO_SCHEMA_REGISTRY_DIR,dbname), getList=False, jsonLong=False)  
  if not jsons: my_error("Cannot get db and partition info from zookeeper for db %s" % dbname) 
  dbg_print("jsons = %s" % jsons)
  db_json = json.loads(jsons)
  return int(db_json["numBuckets"])

def get_espresso_tables(dbname):
  ''' always use zookeeper '''
  table_list= get_json_or_list_from_zookeeper("/%s/schemata/document/%s" % (ESPRESSO_SCHEMA_REGISTRY_DIR,dbname), getList=True)
  if not table_list: my_error("Cannot get table list from zookeeper for db %s" % dbname) 
  #tables = table_list[0].lstrip("[").rstrip("]").split(",")
  tables = convert_zookeeper_list(table_list)
  return tables

def convert_zookeeper_list(zlist):
  dbg_print("zlist = %s" % zlist)
  converted_list = [x.lstrip(" ") for x in zlist[0].lstrip("[").rstrip("]").split(",")]
  return converted_list

def get_storage_nodes(cluster_name):
  ''' always use zookeeper '''
  storage_node_list = get_json_or_list_from_zookeeper("/%s/INSTANCES" % (cluster_name), getList=True)
  if not storage_node_list: my_error("Cannot get storage node list from zookeeper")
  storage_nodes = convert_zookeeper_list(storage_node_list)
  return storage_nodes 

def get_mysql_port(cluster_name,instance_id):
  ''' always use zookeeper '''
  jsons = get_json_or_list_from_zookeeper("/%s/CONFIGS/PARTICIPANT/%s" % (cluster_name, instance_id), getList=False)
  mysql_port = json.loads(jsons)["simpleFields"]["mysql_port"]
  return mysql_port

def get_index_mysql_port(cluster_name,instance_id):
  ''' always use zookeeper '''
  jsons = get_json_or_list_from_zookeeper("/%s/CONFIGS/PARTICIPANT/%s" % (cluster_name, instance_id), getList=False)
  try:
    index_mysql_port = json.loads(jsons)["simpleFields"]["index_mysql_port"]
    return index_mysql_port
  except:
    return ""

def get_index_file_path(cluster_name,instance_id):
  ''' always use zookeeper '''
  jsons = get_json_or_list_from_zookeeper("/%s/CONFIGS/PARTICIPANT/%s" % (cluster_name, instance_id), getList=False)
  try:
    index_root_path = json.loads(jsons)["simpleFields"]["index_root_path"]
    return index_root_path
  except:
    return ""

def get_clusters(helix_url_clusters):
  dbg_print("helix_url_clusters = %s" % helix_url_clusters)
  ret = urllib.urlopen(helix_url_clusters).read()
  #dbg_print("ret = %s" % ret)
  return json.loads(ret)["listFields"]["clusters"]

def get_partition_table_row_count(row_count, node_addr, partition_table_id):
  if node_addr == '%':   # wild card,expect only one
    all_row_count=[(node_addr, row_count[node_addr][partition_table_id][1]) for node_addr in row_count if partition_table_id in row_count[node_addr] ]
    if len(all_row_count) == 1: return all_row_count[0]
    if len(all_row_count) == 0: return -2
    my_error("partition %s more then one master row counts: %s" % (partition_table_id,all_row_count))
  if node_addr in row_count and partition_table_id in row_count[node_addr]:
    return row_count[node_addr][partition_table_id][1]
  else:
    return -2  # did not get any row count

def get_partition_hwm(hwm_relay, master_node_addr, partition_id):
  if master_node_addr in hwm_relay and partition_id in hwm_relay[master_node_addr]:
    return hwm_relay[master_node_addr][partition_id][1]
  else:
    return -2   #did not get any hwm

def get_relay_hwm(relay_node_addr, db, partition):
   relayHost = relay_node_addr.split('_')[0]
   relayPort = relay_node_addr.split('_')[1]
   url = "http://%s:%s/containerStats/inbound/events/psource/%s/%s" % (relayHost, relayPort, db, partition)
   dbg_print("url=%s" % url)
   urlResponse = urllib2.urlopen(url).read()
   dbg_print("urlResponse=%s" % urlResponse)
   jsonResponse = json.loads(urlResponse)
   return int(jsonResponse["maxScn"])

def get_client_hwm(client_node_addr, db, partition):
   url = "http://%s/regops/stats/inbound/callbacks/multiPartitionConsumer_%s_%s" % (client_node_addr, db, partition)
   dbg_print("url=%s" % url)
   urlResponse = urllib2.urlopen(url).read()
   dbg_print("urlResponse=%s" % urlResponse)
   jsonResponse = json.loads(urlResponse)
   return int(jsonResponse["maxSeenWinScn"])

def check_row_count(row_count):
  if options.compare_with_dr:
    push_zk_connstr(options.dr_zookeeper_connstr)
    ret = check_row_count_dr(row_count)
    pop_zk_connstr()
    return ret
  else: 
    return check_row_count_origin(row_count)

def check_row_count_dr(row_count):
  ''' the row count should be all be masters for dr '''
  row_count_match=True
  zero_rows=True
  for db in ESPRESSO_DBS: 
    num_partitions = ESPRESSO_NUM_PARTITIONS[db]
    for table in ESPRESSO_TABLES[db]:
      if ROW_COUNT_TABLES and table not in ROW_COUNT_TABLES: continue
      for partition in range(num_partitions):
        partition_id = "%s_%s" % (db, partition)
        partition_table_id = "%s_%s.%s" % (db, partition, table)
        (origin_master_node_addr, origin_master_row_count) = get_partition_table_row_count(row_count["ORIGIN"], '%', partition_table_id)    # we already choose only master when select row_count for dr
        (dr_master_node_addr, dr_master_row_count) = get_partition_table_row_count(row_count["DR"], '%', partition_table_id)    # we already choose only master when select row_count for dr
        this_partition_mismatch = False
        if origin_master_row_count !=0 or dr_master_row_count !=0: zero_rows=False
        if origin_master_row_count != dr_master_row_count: 
          print "ERROR: row count mismatch for %s => ORIGIN %s : %s, DR %s : %s" % (partition_table_id, origin_master_node_addr, origin_master_row_count, dr_master_node_addr, dr_master_row_count) 
          row_count_match = False
        print_line = "Partition %s => ORIGIN %s : %s" % (partition_table_id, origin_master_node_addr, origin_master_row_count)
        print_line += ", DR %s : %s" % (dr_master_node_addr, dr_master_row_count) 
        if options.verbose: print print_line

  if zero_rows and (not options.expect_zero):
    print "ERROR: zero rows or no rows for tables %s" % (ROW_COUNT_TABLES)
    return RetCode.ERROR
  if options.expect_zero and (not zero_rows):
    print "ERROR: zero rows expected, but found >0 rows for tables %s" % (ROW_COUNT_TABLES)
    return RetCode.ERROR
  if row_count_match: return RetCode.OK         
  else: 
    print "================="
    return RetCode.ERROR
  
def check_row_count_origin(row_count):
  reset_external_view()
  row_count_match=True
  zero_rows=True
  for db in ESPRESSO_DBS: 
    num_partitions = ESPRESSO_NUM_PARTITIONS[db]
    if options.row_count_use_ideal_state:
      external_view = get_ideal_state(options.cluster_name, db)
    else:
      external_view = get_external_view(options.cluster_name, db)
    for table in ESPRESSO_TABLES[db]:
      if ROW_COUNT_TABLES and table not in ROW_COUNT_TABLES: continue
      for partition in range(num_partitions):
        partition_id = "%s_%s" % (db, partition)
        partition_table_id = "%s_%s.%s" % (db, partition, table)
        if partition_id not in external_view or len(external_view[partition_id])==0:
          #my_warning("partition %s does not exist in external view" % partition_id)
          continue
        master_node_addrs = [x for x in external_view[partition_id] if external_view[partition_id][x] == "MASTER"]
        if len(master_node_addrs) ==0:  # no master
          master_row_count=-1
          master_node_addr = "NoMaster"
        else:
          master_node_addr = master_node_addrs[0]
          master_row_count = get_partition_table_row_count(row_count, master_node_addr, partition_table_id)
        print_line = "Partition %s => MASTER %s : %s" % (partition_table_id, master_node_addr, master_row_count)
        #if options.verbose: print "Partition %s  \n  master %s : %s" % (partition_table_id, master_node_addr, master_row_count)
        if master_row_count > 0: zero_rows=False
        slave_node_addrs = [x for x in external_view[partition_id] if external_view[partition_id][x] in ("SLAVE","ERROR")]
        this_partition_mismatch = False
        for storage_node_addr in slave_node_addrs:
          partition_state = external_view[partition_id][storage_node_addr]
          if storage_node_addr == master_node_addr: continue
          slave_row_count = get_partition_table_row_count(row_count, storage_node_addr, partition_table_id)
          if master_row_count !=  slave_row_count: 
            print "ERROR: row count mismatch for %s => MASTER %s : %s, %s %s : %s" % (partition_table_id, master_node_addr, master_row_count, partition_state, storage_node_addr, slave_row_count) 
            row_count_match = False
            this_partition_mismatch = True
          if partition_state == "error": row_count_match = False
          if master_row_count !=  slave_row_count or options.verbose:
            #if partition_state == "MASTER":
            #  print "EV = %s" % json.dumps(external_view, indent=2)
            print_line += ", %s %s : %s" % (partition_state, storage_node_addr, slave_row_count) 
            #print "     slave %s : %s" % (storage_node_addr, slave_row_count) 
        #if this_partition_mismatch or options.verbose: print print_line
        if options.verbose: print print_line
  if zero_rows and (not options.expect_zero):
    print "ERROR: zero rows or no rows for tables %s" % (ROW_COUNT_TABLES)
    return RetCode.ERROR
  if options.expect_zero and (not zero_rows):
    print "ERROR: zero rows expected, but found >0 rows for tables %s" % (ROW_COUNT_TABLES)
    return RetCode.ERROR
  if row_count_match: return RetCode.OK         
  else: 
    print "================="
    return RetCode.ERROR

def get_partition_table_index_count(index_count, node_addr, partition_table_id):
  if node_addr in index_count and partition_table_id in index_count[node_addr]: 
     return (index_count[node_addr][partition_table_id][1], index_count[node_addr][partition_table_id][2])
  else:
     return (-2, -2)

def check_index_count(index_count):
  if options.use_lucene_mysql == False and options.use_lucene_filesystem == False:
     print "Index is not being used. Returning OK"
     return RetCode.OK

  reset_external_view()
  index_count_db_match=True
  index_count_file_match=True
  zero_rows=True
  for db in ESPRESSO_DBS: 
    num_partitions = ESPRESSO_NUM_PARTITIONS[db]
    external_view = get_external_view(options.cluster_name, db)
    for table in ESPRESSO_TABLES[db]:
      if ROW_COUNT_TABLES and table not in ROW_COUNT_TABLES: continue
      for partition in range(num_partitions):
        partition_id = "%s_%s" % (db, partition)
        partition_table_id = "%s_%s.%s" % (db, partition, table)
        if partition_id not in external_view or len(external_view[partition_id])==0:
          #my_warning("partition %s does not exist in external view" % partition_id)
          continue
        dbg_print ("Checking Partition %s for index docCount ..." % (partition_table_id))
        master_node_addrs = [x for x in external_view[partition_id] if external_view[partition_id][x] == "MASTER"]
        if len(master_node_addrs) ==0:  # no master
          master_index_count_db=-1
          master_index_count_file=-1
          master_node_addr = "NoMaster"
        else:
          master_node_addr = master_node_addrs[0]
          (master_index_count_db, master_index_count_file) = get_partition_table_index_count(index_count, master_node_addr,partition_table_id)
        print_line = "Partition %s => MASTER %s : DB %s FILE %s" % (partition_table_id, master_node_addr, master_index_count_db, master_index_count_file)
        if master_index_count_db > 0: zero_rows=False
        if master_index_count_file > 0: zero_rows=False
        slave_node_addrs = [x for x in external_view[partition_id] if external_view[partition_id][x] in ("SLAVE","ERROR")]
        this_partition_mismatch = False
        for storage_node_addr in slave_node_addrs:
          partition_state = external_view[partition_id][storage_node_addr]
          if storage_node_addr == master_node_addr: continue
          (slave_index_count_db, slave_index_count_file) = get_partition_table_index_count(index_count, storage_node_addr,partition_table_id)
          if master_index_count_db !=  slave_index_count_db: 
            print "ERROR: row count mismatch for %s => MASTER DB %s : %s, %s %s : %s" % (partition_table_id, master_node_addr, master_index_count_db, partition_state, storage_node_addr, slave_index_count_db) 
            index_count_db_match = False
            this_partition_mismatch = True
          if master_index_count_file !=  slave_index_count_file: 
            print "ERROR: row count mismatch for %s => MASTER FILE %s : %s, %s %s : %s" % (partition_table_id, master_node_addr, master_index_count_file, partition_state, storage_node_addr, slave_index_count_file) 
            index_count_file_match = False
            this_partition_mismatch = True

          if partition_state == "error": index_count_match = False
          if master_index_count_db !=  slave_index_count_db or master_index_count_file != slave_index_count_file or options.verbose:
            #if partition_state == "MASTER":
            #  print "EV = %s" % json.dumps(external_view, indent=2)
            print_line += ", %s %s : DB %s FILE %s" % (partition_state, storage_node_addr, slave_index_count_db, slave_index_count_file) 
            #print "     slave %s : %s" % (storage_node_addr, slave_row_count) 
        #if this_partition_mismatch or options.verbose: print print_line
        if options.verbose: print print_line
  if zero_rows and (not options.expect_zero):
    print "ERROR: zero index documents for tables %s" % (ROW_COUNT_TABLES)
    return RetCode.ERROR
  if options.expect_zero and (not zero_rows):
    print "ERROR: zero indices expected, but found >0 indices for tables %s" % (ROW_COUNT_TABLES)
    return RetCode.ERROR
  if index_count_db_match and index_count_file_match: return RetCode.OK         
  else: 
    print "================="
    return RetCode.ERROR

import Queue
import threading
thread_pool_queue=Queue.Queue()
thread_pool_queue_index=Queue.Queue()
thread_pool_hwm_relay_queue=Queue.Queue()
row_count_queue=Queue.Queue()   # to hold the row count
index_count_queue=Queue.Queue()
hwm_relay_queue=Queue.Queue()
class worker_thread(threading.Thread):
  ''' worker thread of a thread pool '''
  def run(self):
    while True: 
      (qry, hostname, port, storage_node_addr,partition_table_id, master_or_slave) = thread_pool_queue.get()
      
      count = mysql_exec_sql_one_row(qry, dbname=None, user="rplespresso", passwd="espresso", host=hostname, port=str(port))
      row_count_queue.put((storage_node_addr, partition_table_id, master_or_slave, count[0]))
      thread_pool_queue.task_done()
      #dbg_print("task done: %s, queue=%s, %s " % (ret, row_count_queue, row_count_queue.qsize()))

'''
This worker thread finds the number of index document counts in lucene and mysql index
For this call to work properly, you must copy the following directory from trunk into each storage node under /export/apps/espresso/espresso-tools, and give execute permissions to all user
	build/espresso-tools-pkg/distributions/espresso-tools-pkg-<version>-SNAPSHOT.tar.gz
After un-tar .tar.gz file, each storage node must have the following file: /export/apps/espresso/espresso-tools/bin/index-docCount-getter.sh
'''
class worker_thread_index(threading.Thread):
  ''' worker thread of a thread pool '''
  def run(self):
    expr = re.compile("DocCount (\d+)")
    while True: 
      (storage_node_addr,db, table, partition_num, hostname, port, lucene_dir, master_or_slave) = thread_pool_queue_index.get()
      countdb = 0
      countfile = 0
      curl_url = ("http://%s:19000/%s/%s/%s" % (hostname,db,table,partition_num))
      curl_data = {}
      curl_data['cmd'] = "docCountGetter"
      
      if options.use_lucene_mysql:
         curl_data['arg1'] = "txnMysql"
         curl_data['arg2'] = ("%s" % str(port))
      if options.use_lucene_filesystem:
         curl_data['arg1'] = "fs"
         curl_data['arg2'] = ("%s" % lucene_dir)

      isSuccessful = False
      retryCount = 0
      count_out = ""

      while ( isSuccessful == False ):
        urlresponse=None 
        try:
           #replace single quotes with double quotes, and remove unicode demarker
           dataStr = str(curl_data).replace("'","\"")
           dataStr = dataStr.replace("u\"", "\"")
           dbg_print ("Calling %s with data_str %s" % (curl_url, dataStr))
           urlresponse = urllib2.urlopen(curl_url, dataStr)
        except urllib2.HTTPError, err:
           if err.code == 400: #Test service gives a 400 error back for some reason even for success.. need to fix that later
             count_out = err.headers['result']
             dbg_print ("Successfully called %s with DataStr %s and Received result %s" % (curl_url, curl_data, count_out))
           else:
             retryCount = retryCount + 1
             if ( retryCount > 3 ):
               print ("ERROR: Host %s call to docCountGetter returned HTTP error. GIVING UP." % (curl_url))
               raise err
             print ("WARNING: Host %s call to docCountGetter returned HTTP error. RETRYING. Error: %s ::curl_data:%s::curl_url:%s" % (hostname, err,curl_data,curl_url))
             time.sleep(5)
             continue
        except Exception, err2:
             retryCount = retryCount + 1
             if ( retryCount > 3 ):
                print ("ERROR: Host %s call to docCountGetter returned Generic error. GIVING UP" % (curl_url))
                raise err2
             print ("WARNING: Host %s call to docCountGetter returned generic error. RETRYING. Error: %s ::curl_data:%s::curl_url:%s" % (hostname, err2,curl_data,curl_url))
             time.sleep(5)
             continue
        finally:
           if urlresponse: urlresponse.close()

        count=0
        count_obj = expr.search(count_out)
        if count_obj != None:
           count=int(count_obj.group(1))
           isSuccessful = True
        else:
           retryCount = retryCount + 1
           if ( retryCount > 3 ):
             raise Exception("ERROR!! Host: %s call to docCountGetter returned: %s" % (curl_url, count_out))
           print ("WARNING: Host %s call to docCountGetter returned: %s. ::curl_data:%s::curl_url:%s RETRYING.." % (hostname, count_out,curl_data,curl_url))
           time.sleep(5)
           continue

        if options.use_lucene_mysql:
           countdb = count
        if options.use_lucene_filesystem:
           countfile = count

      index_count_queue.put((storage_node_addr, db, table, partition_num, master_or_slave, countdb, countfile))
      isSuccessful = True
      dbg_print ("Signalling task_done on %s.%s partition %s (%s) DBCount %s; FileCount %s on SN %s in index_count_queue" % (db, table, partition_num, master_or_slave, countdb, countfile, storage_node_addr))
      thread_pool_queue_index.task_done()

class worker_thread_hwm(threading.Thread):
  ''' worker thread of a thread pool '''
  def run(self):
    while True: 
      (qry, hostname, port, storage_node_addr,partition_id, master_or_slave) = thread_pool_hwm_relay_queue.get()
      hwm = mysql_exec_sql_one_row(qry, dbname=None, user="rplespresso", passwd="espresso", host=hostname, port=str(port))
      hwm_relay_queue.put((storage_node_addr, partition_id, master_or_slave, hwm[0]))
      thread_pool_hwm_relay_queue.task_done()

def register_avro_checksum_table():
  '''select count(*) from mysql.avro_schema_defs where name='es_checksum';'''
  live_instances_list = get_live_instances(options.cluster_name)
  get_avro_sh_dir = this_file_dirname
  libdir = os.path.join(os.path.dirname(this_file_dirname), "lib")
  if not os.path.exists(libdir):  # called from integration-test/script
    get_avro_sh_dir = os.path.join(get_view_root(),"build/espresso-tools-pkg/exploded/bin")
  get_avro_sh = os.path.join(get_avro_sh_dir, "espresso_checksum_table_avro.sh")
  avroSchema = sys_pipe_call(get_avro_sh) #call ChecksumDocumentSchemaImpl to get the avro schema for checksum_esp table
  
  ''' mysql does not like when the avro schema contains doc and version info - so remove those '''
  patternDoc = re.compile('\"doc\"\s*:\s*\"[\w\s]*\"\,')
  avroSchema = patternDoc.sub("",avroSchema)
  patternVer = re.compile('\"version\"\s*:\s*[\d]+\,')
  avroSchema = patternVer.sub("",avroSchema)
  dbg_print (avroSchema)
  
  sqlQry = ("set @schema_def := '%s';select avro_register_schema(\"es_checksum\",@schema_def);show warnings;" % (avroSchema))
  filename = ("%s/%s" % (this_file_dirname, "avro_espresso_checksum_table.sql"))
  with open(filename, "w") as avro_schema_f:
    avro_schema_f.write(sqlQry)
  
  for live_instance in live_instances_list:
     mysqlhost = live_instance.split("_")[0]
     storage_node_index = ESPRESSO_STORAGE_NODE_ADDRS.index(live_instance)
     mysqlport = MASTER_MYSQL_PORTS[storage_node_index]
     dbg_print ("%s = %s:%s" % (live_instance, mysqlhost, mysqlport))
     ret = mysql_exec_from_file(filename, dbname=None, user="espresso", passwd="espresso", host=mysqlhost, port=str(mysqlport))
     print (str(ret))

def trigger_consistency_check():
  helixHost = options.helix_host
  helixPort = options.helix_port
  clusterName = options.cluster_name
  
  ''' Loop through the database and issue consistency checker request '''
  db_names = get_db_names(clusterName)
  for db_name in db_names:
    external_view = get_external_view(options.cluster_name, db_name)
    for partition_name in external_view:
        for instance_name in  external_view[partition_name]:
            partition_state = external_view[partition_name][instance_name]
            ''' Run Consistency Check against all master partitions '''
            if partition_state == "MASTER":
                partitionId = int(partition_name.split("_")[1])

                ''' Trigger MySqlChecksumGenerator for MASTER partition '''
                dbg_print ("Requesting MySqlChecksumGenerator for DB %s, PartitionID %s on Instance %s" % (db_name, partitionId, instance_name))
                sys_call('%s/espresso_backup_restore_cmd.py -c MySqlChecksumGenerator --cluster_name %s --instance_name %s --dbname %s --partition_id %s --partition_state %s --host %s --port %s' % (this_file_dirname, clusterName, instance_name, db_name, partitionId, partition_state, helixHost, helixPort))

def verify_consistency_check():
  retval = True
  for db in ESPRESSO_DBS: 
    num_partitions = ESPRESSO_NUM_PARTITIONS[db]
    external_view = get_external_view(options.cluster_name, db)

    get_sh_dir = this_file_dirname
    libdir = os.path.join(os.path.dirname(this_file_dirname), "lib")
    if not os.path.exists(libdir):  # called from integration-test/script
      get_sh_dir = os.path.join(get_view_root(),"build/espresso-tools-pkg/exploded/bin")
    get_constants_sh = os.path.join(get_sh_dir, "espresso_constants.sh")
    table = sys_pipe_call("%s checksumTableName" % (get_constants_sh)) # get checksum table name
 
    for partition in range(num_partitions):
      partition_id = "%s_%s" % (db, partition)
      partition_table_id = "%s_%s.%s" % (db, partition, table)
      if partition_id not in external_view or len(external_view[partition_id])==0:
        continue
      for storage_node_addr in external_view[partition_id]:
        storage_node_index = ESPRESSO_STORAGE_NODE_ADDRS.index(storage_node_addr)
	qry = "select tableName,avro_fetch(\"es_checksum\",val,\"errorString\") as errorString from es_%s GROUP BY 1,2 ORDER BY 1,2" % (partition_table_id)
        port = MASTER_MYSQL_PORTS[storage_node_index]
        hostname = storage_node_addr.split("_")[0]
        master_or_slave=external_view[partition_id][storage_node_addr]
        results=mysql_exec_sql(qry, dbname=None, user="espresso", passwd="espresso", host=hostname, port=str(port), do_split=True)
        for res in results:
           if res[1] != "NULL":
              print res
              print("ERROR: Consistency Check Failed for DB %s, Table %s, on %s:%s. errorString is %s" % (db, res[0], hostname, str(port), res[1]))
              retval = False
  return retval

def get_row_count_start_thread_pool():
  for i in range(options.thread_pool_size):   # thread pool 
    t = worker_thread()
    t.daemon = True
    t.start()

def get_hwm_start_thread_pool():
  for i in range(options.thread_pool_size):   # thread pool 
    t = worker_thread_hwm()
    t.daemon = True
    t.start()

def get_row_count():
  if options.compare_with_dr:
    return get_row_count_dr()
  else:
    return get_row_count_origin()

def get_row_count_helper(cluster_name, espresso_dbs,espresso_tables, espresso_num_partitions,espresso_storage_node_addrs,master_mysql_ports,master_only=False):
  reset_external_view()
  row_count={}
  for storage_node_addr in espresso_storage_node_addrs: row_count[storage_node_addr]={}
  for db in espresso_dbs: 
    num_partitions = espresso_num_partitions[db]
    if options.row_count_use_ideal_state:
      external_view = get_ideal_state(cluster_name, db)
    else:
      external_view = get_external_view(cluster_name, db)
    for table in espresso_tables[db]:
      if ROW_COUNT_TABLES and table not in ROW_COUNT_TABLES: continue
      for partition in range(num_partitions):
        partition_id = "%s_%s" % (db, partition)
        partition_table_id = "%s_%s.%s" % (db, partition, table)
        if partition_id not in external_view or len(external_view[partition_id])==0:
          continue
        if master_only:
          row_count_node_addrs = [x for x in external_view[partition_id] if external_view[partition_id][x] == "MASTER"]
        else:
          row_count_node_addrs = external_view[partition_id]
        for storage_node_addr in row_count_node_addrs:
          storage_node_index = espresso_storage_node_addrs.index(storage_node_addr)
	  qry = "select count(*) from es_%s" % (partition_table_id)
          port = master_mysql_ports[storage_node_index]
          hostname = storage_node_addr.split("_")[0]
          #master_or_slave=external_view[partition_id][storage_node_addr][0]  # First Char
          master_or_slave=external_view[partition_id][storage_node_addr]
          thread_pool_queue.put((qry, hostname, port, storage_node_addr,partition_table_id, master_or_slave))
  thread_pool_queue.join()
  #dbg_print("row count queue=%s, %s " % (row_count_queue, row_count_queue.qsize()))
  while not row_count_queue.empty():
    (storage_node_addr, partition_table_id, master_or_slave, count) = row_count_queue.get()
    #print "%s, %s, %s" % (storage_node_addr, partition_table_id, ret)
    row_count[storage_node_addr][partition_table_id]=(master_or_slave, count)
  dbg_print("row_count = \n%s" % pprint.pformat(row_count, width=160))
  return row_count
     
def get_row_count_origin():
  return  get_row_count_helper(options.cluster_name, ESPRESSO_DBS,ESPRESSO_TABLES,ESPRESSO_NUM_PARTITIONS,ESPRESSO_STORAGE_NODE_ADDRS,MASTER_MYSQL_PORTS)

def get_row_count_dr():
  origin_row_count = get_row_count_helper(options.cluster_name, ESPRESSO_DBS, ESPRESSO_TABLES, ESPRESSO_NUM_PARTITIONS,ESPRESSO_STORAGE_NODE_ADDRS,MASTER_MYSQL_PORTS,True)
  push_zk_connstr(options.dr_zookeeper_connstr)
  dr_row_count = get_row_count_helper(options.dr_cluster_name, ESPRESSO_DBS, ESPRESSO_TABLES, ESPRESSO_NUM_PARTITIONS,DR_ESPRESSO_STORAGE_NODE_ADDRS,DR_MASTER_MYSQL_PORTS,True)
  row_count={"ORIGIN":origin_row_count, "DR": dr_row_count}
  pop_zk_connstr()
  return row_count

def cmd_waitfor_row_count():
  ''' Wait for the row count until they match '''
  def wait_for_row_count():
    row_count = get_row_count()
    return check_row_count(row_count) == RetCode.OK
  ret = wait_for_condition_1(wait_for_row_count, timeout=options.retryTimeout, sleep_interval=options.retryInterval)
  if ret == RetCode.OK: print "OK: master slave row count match"
  elif ret == RetCode.TIMEOUT: print "TIMEOUT: master slave row count does not match or partition in ERROR state after %s sec" % options.retryTimeout
  #if options.verbose: print "row_count = \n%s" % pprint.pformat(get_row_count())
  return ret

def cmd_check_row_count():
  ''' Check if row count of master slave matches '''
  if options.retryTimeout: return cmd_waitfor_row_count()
  #check_helix_ret = cmd_check_helix()
  row_count = get_row_count()
  if options.verbose: print "row_count = \n%s" % pprint.pformat(row_count, width=160)
  ret = check_row_count(row_count)
  if ret == RetCode.OK: print "OK: master slave row count match"
  else: print "ERROR: master slave row count does not match or partition in ERROR state"
  #if check_helix_ret: print "ERROR: helix state error"
  #return ret or check_helix_ret

def get_index_count_start_thread_pool():
  for i in range(options.thread_pool_size):   # thread pool 
    t = worker_thread_index()
    t.daemon = True
    t.start()

def get_index_count():
  index_count={}
  num=0
  for storage_node_addr in ESPRESSO_STORAGE_NODE_ADDRS: index_count[storage_node_addr]={}
  for db in ESPRESSO_DBS: 
    num_partitions = ESPRESSO_NUM_PARTITIONS[db]
    external_view = get_external_view(options.cluster_name, db)
    for table in ESPRESSO_TABLES[db]:
      if ROW_COUNT_TABLES and table not in ROW_COUNT_TABLES: continue
      for partition in range(num_partitions):
        partition_id = "%s_%s" % (db, partition)
        partition_table_id = "%s_%s.%s" % (db, partition, table)
        if partition_id not in external_view or len(external_view[partition_id])==0:
          continue
        for storage_node_addr in external_view[partition_id]:
          storage_node_index = ESPRESSO_STORAGE_NODE_ADDRS.index(storage_node_addr)
	  port = MASTER_INDEX_MYSQL_PORTS[storage_node_index]
	  lucene_dir = MASTER_INDEX_FILE_PATH[storage_node_index]
          hostname = storage_node_addr.split("_")[0]
          #master_or_slave=external_view[partition_id][storage_node_addr][0]  # First Char
          master_or_slave=external_view[partition_id][storage_node_addr]
          thread_pool_queue_index.put((storage_node_addr,db, table, partition, hostname, port, lucene_dir, master_or_slave))
          dbg_print ("Put %s.%s Partition %s (%s) on SN %s into thread_pool_queue" % (db, table, partition, master_or_slave, storage_node_addr))
  thread_pool_queue_index.join()
  dbg_print ("Join Complete on thread_pool_queue_index")
  #dbg_print("row count queue=%s, %s " % (row_count_queue, row_count_queue.qsize()))
  while not index_count_queue.empty():
    (storage_node_addr, db, table, partition, master_or_slave, countdb, countfile) = index_count_queue.get()
    partitionId = ("%s_%s.%s" % (db, partition, table))
    dbg_print ("Processing Partition %s" % (partitionId))
    index_count[storage_node_addr][partitionId]=(master_or_slave, countdb, countfile)
  return index_count

def cmd_waitfor_index_count():
  ''' Wait for the row count until they match '''
  def wait_for_index_count():
    index_count = get_index_count()
    return check_index_count(index_count) == RetCode.OK
  ret = wait_for_condition_1(wait_for_index_count, timeout=options.retryTimeout, sleep_interval=options.retryInterval)
  if ret == RetCode.OK: print "OK: master slave index count match"
  elif ret == RetCode.TIMEOUT: print "TIMEOUT: master slave index count does not match or partition in ERROR state after %s sec" % options.retryTimeout
  #if options.verbose: print "index_count = \n%s" % pprint.pformat(get_index_count())
  return ret

def cmd_check_index_count():
  if options.use_lucene_mysql == False and options.use_lucene_filesystem == False:
     print "Index is not being used. Returning OK"
     return RetCode.OK

  ''' Check if index count of master slave matches '''
  if options.retryTimeout: return cmd_waitfor_index_count()
  index_count = get_index_count()
  if options.verbose: print "index_count = \n%s" % pprint.pformat(index_count, width=160)
  ret = check_index_count(index_count)
  if ret == RetCode.OK: print "OK: master slave index count match"
  else: print "ERROR: master slave index count does not match or partition in ERROR state"
  return ret

def check_hwm_client(hwm_client, client_node_addr):
  reset_external_view()
  hwm_client_match=True
  dblist = [];
  dblist.append(options.subs)
  for db in dblist: 
    num_partitions = ESPRESSO_NUM_PARTITIONS[db]
    if options.row_count_use_ideal_state:
      external_view = get_ideal_state(db)
    else:
      external_view = get_external_view(options.cluster_name, db)

    for partition in range(num_partitions):
        partition_id = "%s_%s" % (db, partition)
        if partition_id not in external_view or len(external_view[partition_id])==0:
          #my_warning("partition %s does not exist in external view" % partition_id)
          continue
        storage_node_addrs = [x for x in external_view[partition_id] if external_view[partition_id][x] == "MASTER"]

        if len(storage_node_addrs) ==0:  # no master
          master_hwm_mysql=-1
          master_node_addr = "NoMaster"
          this_partition_mismatch = True
        else:
          for storage_node_addr in storage_node_addrs:
            master_hwm_mysql = get_partition_hwm(hwm_client, storage_node_addr, partition_id)
            partition_state = external_view[partition_id][storage_node_addr]
            print_line = "Partition %s => %s %s : %s" % (partition_id, partition_state, storage_node_addr, master_hwm_mysql)
            this_partition_mismatch = False

            slave_hwm_client = get_client_hwm(client_node_addr, db, partition)
            seq_id = long(master_hwm_mysql) & 4294967295  # get the least significant 32 bits
            if slave_hwm_client == 0: 
                #if master mysql sequence id is 1, this is OK
                #maxSCN of 0 in relay means that no events are received from master. This is OK
                if seq_id <= 1:
                   if options.verbose: print "WARNING: maxSCN in client is 0. Ignoring since master mySql is at sequence 1"
                   slave_hwm_client = master_hwm_mysql   #in client, hwm 0 means no events are received, which matches generation n, sequence 1
            if long(master_hwm_mysql) !=  long(slave_hwm_client): 
                 if seq_id >1:
                   print "ERROR: HWM mismatch for %s => %s %s : %s, client %s : %s" % (partition_id, partition_state, storage_node_addr, master_hwm_mysql, client_node_addr, slave_hwm_client) 
                   hwm_client_match = False
                   this_partition_mismatch = True
                 else:
                   print "WARNING: HWM mismatch but seq_id=1 for %s => %s %s : %s, client %s : %s" % (partition_id, partition_state, storage_node_addr, master_hwm_mysql, client_node_addr, slave_hwm_client) 
            if this_partition_mismatch == False or options.verbose:
                    print_line2 = print_line + ", Client partition(%s) : %s" % (partition_id, slave_hwm_client) 
                    if options.verbose: print print_line2
  if hwm_client_match: return RetCode.OK         
  else: 
    print "================="
    return RetCode.ERROR

def check_hwm_relay(hwm_relay):
  reset_external_view()
  hwm_relay_match=True
  for db in ESPRESSO_DBS: 
    num_partitions = ESPRESSO_NUM_PARTITIONS[db]
    if options.row_count_use_ideal_state:
      external_view = get_ideal_state(db)
    else:
      external_view = get_external_view(options.cluster_name, db)
    external_view_relay = get_external_view(options.relay_cluster_name, db)
    for partition in range(num_partitions):
        partition_id = "%s_%s" % (db, partition)
        if partition_id not in external_view or len(external_view[partition_id])==0:
          #my_warning("partition %s does not exist in external view" % partition_id)
          continue
        if options.check_master_only:
          storage_node_addrs = [x for x in external_view[partition_id] if external_view[partition_id][x] == "MASTER"]
        else:
          storage_node_addrs = [x for x in external_view[partition_id]]
        if len(storage_node_addrs) ==0:  # no master
          master_hwm_mysql=-1
          master_node_addr = "NoMaster"
          this_partition_mismatch = True
        else:
          for storage_node_addr in storage_node_addrs:
            master_hwm_mysql = get_partition_hwm(hwm_relay, storage_node_addr, partition_id)
            partition_state = external_view[partition_id][storage_node_addr]
            print_line = "Partition %s => %s %s : %s" % (partition_id, partition_state, storage_node_addr, master_hwm_mysql)
            relay_key = "%s,%s,p%s_1,%s" % (storage_node_addr,db,partition,partition_state)
            relay_node_addrs = [x for x in external_view_relay[relay_key]]
            this_partition_mismatch = False
            for relay_node_addr in relay_node_addrs:
              slave_hwm_relay = get_relay_hwm(relay_node_addr, db, partition)
              seq_id = long(master_hwm_mysql) & 4294967295  # get the least significant 32 bits
              if slave_hwm_relay == 0: 
                 #if master mysql sequence id is 1, this is OK
                 #maxSCN of 0 in relay means that no events are received from master. This is OK
                 if seq_id <= 1:
                    if options.verbose: print "WARNING: maxSCN in relay is 0. Ignoring since master mySql is at sequence 1"
                    slave_hwm_relay = master_hwm_mysql   #in relay, hwm 0 means no events are received, which matches generation n, sequence 1
              if long(master_hwm_mysql) !=  long(slave_hwm_relay): 
                 if seq_id >1:
                   print "ERROR: HWM mismatch for %s => %s %s : %s, RELAY %s : %s" % (partition_id, partition_state, storage_node_addr, master_hwm_mysql, relay_node_addr, slave_hwm_relay) 
                   hwm_relay_match = False
                   this_partition_mismatch = True
                 else:
                   print "WARNING: HWM mismatch but seq_id=1 for %s => %s %s : %s, RELAY %s : %s" % (partition_id, partition_state, storage_node_addr, master_hwm_mysql, relay_node_addr, slave_hwm_relay) 
              if this_partition_mismatch == False or options.verbose:
                    print_line2 = print_line + ", RELAY %s : %s" % (relay_node_addr, slave_hwm_relay) 
                    if options.verbose: print print_line2
  if hwm_relay_match: return RetCode.OK         
  else: 
    print "================="
    return RetCode.ERROR


def cmd_check_hwm_client():
  if options.retryTimeout: return cmd_waitfor_hwm_client()
  hwm = get_hwm()
  if options.verbose: print "HWM = \n%s" % pprint.pformat(hwm, width=160)
  ret = check_hwm_client(hwm, options.dbus_client_addr)
  if ret == RetCode.OK: print "OK: client HWM match"
  else: print "ERROR: client HWM does not match or partition in ERROR state"
  return ret

def cmd_waitfor_hwm_client():
  ''' Wait for the row count until they match '''
  def wait_for_hwm_client():
    hwm = get_hwm()
    return check_hwm_client(hwm, options.dbus_client_addr) == RetCode.OK
  ret = wait_for_condition_1(wait_for_hwm_client, timeout=options.retryTimeout, sleep_interval=options.retryInterval)
  if ret == RetCode.OK: print "OK: client HWM match"
  elif ret == RetCode.TIMEOUT: print "TIMEOUT: client HWM does not match or partition in ERROR state after %s sec" % options.retryTimeout
  #if options.verbose: print "HWM = \n%s" % pprint.pformat(get_hwm())
  return ret


def cmd_check_hwm_relay():
  ''' Check if index count of master slave matches '''
  if options.retryTimeout: return cmd_waitfor_hwm_relay()
  hwm = get_hwm()
  if options.verbose: print "HWM = \n%s" % pprint.pformat(hwm, width=160)
  ret = check_hwm_relay(hwm)
  if ret == RetCode.OK: print "OK: master and relay HWM match"
  else: print "ERROR: master and relay HWM does not match or partition in ERROR state"
  return ret

def cmd_waitfor_hwm_relay():
  ''' Wait for the row count until they match '''
  def wait_for_hwm_relay():
    hwm = get_hwm()
    return check_hwm_relay(hwm) == RetCode.OK
  ret = wait_for_condition_1(wait_for_hwm_relay, timeout=options.retryTimeout, sleep_interval=options.retryInterval)
  if ret == RetCode.OK: print "OK: master and relay HWM match"
  elif ret == RetCode.TIMEOUT: print "TIMEOUT: master and relay HWM does not match or partition in ERROR state after %s sec" % options.retryTimeout
  #if options.verbose: print "HWM = \n%s" % pprint.pformat(get_hwm())
  return ret

def get_hwm():
  hwm_relay={}
  for storage_node_addr in ESPRESSO_STORAGE_NODE_ADDRS: hwm_relay[storage_node_addr]={}

  for db in ESPRESSO_DBS: 
    num_partitions = ESPRESSO_NUM_PARTITIONS[db]
    external_view = get_external_view(options.cluster_name, db)
    for partition in range(num_partitions):
      partition_id = "%s_%s" % (db, partition)
      if partition_id not in external_view or len(external_view[partition_id])==0:
          continue
      for storage_node_addr in external_view[partition_id]:
          storage_node_index = ESPRESSO_STORAGE_NODE_ADDRS.index(storage_node_addr)
          # dzhang: the returned local scn is the next scn, current should be the query -1
          qry = "select (get_binlog_gen_id('es_%s') << 32) + get_binlog_seq_id('es_%s')-1" % (partition_id, partition_id)
          port = MASTER_MYSQL_PORTS[storage_node_index]
          hostname = storage_node_addr.split("_")[0]
          master_or_slave=external_view[partition_id][storage_node_addr]
          thread_pool_hwm_relay_queue.put((qry, hostname, port, storage_node_addr,partition_id, master_or_slave))
  thread_pool_hwm_relay_queue.join()
  #dbg_print("row count queue=%s, %s " % (row_count_queue, row_count_queue.qsize()))
  while not hwm_relay_queue.empty():
    (storage_node_addr, partition_id, master_or_slave, hwm) = hwm_relay_queue.get()
    #print "%s, %s, %s" % (storage_node_addr, partition_table_id, ret)
    hwm_relay[storage_node_addr][partition_id]=(master_or_slave, hwm)
  dbg_print("HWM = \n%s" % pprint.pformat(hwm_relay, width=160))
  return hwm_relay

def cmd_run_consistency_check():
  ''' Register avro schema for checksum_esp '''
  register_avro_checksum_table()

  ''' Trigger Consistency Check for each MASTER partitions '''
  trigger_consistency_check()

  ''' Check if master and slave nodes are consistent '''
  ret = verify_consistency_check()

  if ret != True:
     print "Consistency Check Failed"
     return RetCode.ERROR

  print "Consistency Check Successfully Completed"
  return RetCode.OK


def cmd_list_partitions():
  ''' List the partitions. Just like external view. with a filter '''
  print "List state for cluster %s" % options.cluster_name
  db_names = get_db_names(options.cluster_name)
  for db_name in db_names:
    external_view = get_external_view(options.cluster_name, db_name)
    for partition_name in external_view:
      for instance_name in  external_view[partition_name]:
	state = external_view[partition_name][instance_name]
	if not options.state_to_list  or state == options.state_to_list:
	   print "%s : %s : %s : %s " % (db_name, partition_name, instance_name, state)

def cmd_repair():
  ''' try to repair it '''
  ret = RetCode.OK
  if len(PartitionNoMaster) > 0 or len(PartitionErrorState) > 0 or options.force_repair:
    check_helix_status()
    #dry_run_str = options.dry_run and "dryrun" or "writetozk"
    #sys_call("%s/run_class.sh com.linkedin.espresso.tools.cluster.RepairHWMEntry %s %s IdealState %s" % (this_file_dirname, options.zookeeper_connstr, options.cluster_name, dry_run_str))
    #sys.exit(1)
    #  print("Trying to repair the following partitions has no master: %s" % PartitionNoMaster)
    print("Trying to repair the above issues")
    print "Run FixIdealState with dryrun options, only zk mode is supported"
    sys_ret = sys_pipe_call("%s/run_class.sh com.linkedin.espresso.tools.cluster.FixIdealState %s %s zk dryrun" % (this_file_dirname, options.zookeeper_connstr, options.cluster_name))
    if re.search("No valid assignment found", sys_ret): 
      print "ERROR: FixIdealState found No valid assignment! Please Disable Slave/Master and run backup/restore, Then try to fix it again"
      return RetCode.ERROR
    else:
      print "FixIdealState found valid assignment! Starts fixing the clusters"
      if ask_to_continue("Stop slave and master based on EV "): repair_stop_slave_master()
      dry_run_str = options.dry_run and "dryrun" or "writetozk"
      if ask_to_continue("FixIdeaState with %s option  " % dry_run_str):
        sys_call("%s/run_class.sh com.linkedin.espresso.tools.cluster.FixIdealState %s %s zk %s" % (this_file_dirname, options.zookeeper_connstr, options.cluster_name, dry_run_str))
	# refresh the IdealState 
	analyze_cluster_status_refresh_ev_is()
	print "New IdealState = \n%s" % pprint.pformat(IdealStates) 

      if ask_to_continue("FixHWM with IS as source with %s option " % dry_run_str):
        dry_run_str = options.dry_run and "dryrun" or "fix"
        sys_call("%s/run_class.sh com.linkedin.espresso.tools.cluster.RepairHWMEntry %s %s IdealState %s" % (this_file_dirname, options.zookeeper_connstr, options.cluster_name, dry_run_str))
      if ask_to_continue("Enable slave and master based on IS "): repair_enable_slave_master()
      print "Wait for replicaiton to setup and check status"
      print "Finished repair the cluster. Please run check_status and check_mysql_status"
  elif len(PartitionWrongNumberofSlaves) > 0:
    print "Disable/stop the slave"
    print "Compute difference in SCN"
    print "Either Run binlog undo/Bounce Slave or backup/resotre run catchup After Bootstrap"
  else:
    print "Cluster is OK, no need to repair"
    
  my_exit(ret)

def repair_slave_master_find_instances(instance_list, state, ev_or_is_state_count):
  ''' find the instances in a given state. Input is a list '''
  ret_list = []
  for db_name in ev_or_is_state_count:
    for instance in instance_list:
      if instance in ev_or_is_state_count[db_name] and state in ev_or_is_state_count[db_name][instance] and instance not in ret_list: # has the state
        ret_list.append(instance)
  return ret_list

def repair_stop_slave_master():
  ''' from EV, find the group of nodes. stop them. stop all the ERROR node first '''
  for instance_error_group in InstanceErrorGroups:
    instances_in_the_group = instance_error_group.split(",") 
    error_instances = repair_slave_master_find_instances(instances_in_the_group, "ERROR", StateCountDictEV)
    if error_instances and ask_to_continue("Please stop the ERROR instances %s " % error_instances):
      pass  # should wait here, or do the actual stop
    slave_instances = repair_slave_master_find_instances(instances_in_the_group, "SLAVE", StateCountDictEV)
    slave_instances = [x for x in slave_instances if x not in error_instances]
    if slave_instances and ask_to_continue("Please stop the SLAVE instances %s " % slave_instances):
      pass  # should wait here, or do the actual stop
    master_instances = repair_slave_master_find_instances(instances_in_the_group, "MASTER", StateCountDictEV)
    master_instances = [x for x in master_instances if x not in error_instances and x not in slave_instances]
    if master_instances and ask_to_continue("Please stop the SLAVE instances %s " % master_instances):
      pass  # should wait here, or do the actual stop
 
def repair_enable_slave_master():
  ''' from IS, find the group of nodes. start the nodes, slave first '''
  # need to reload is
  analyze_cluster_status()
  for instance_error_group in InstanceErrorGroups:
    instances_in_the_group = instance_error_group.split(",") 
    master_instances = repair_slave_master_find_instances(instances_in_the_group, "MASTER", StateCountDictIS)
    if master_instances and ask_to_continue("Please start the MASTER instances %s  " % master_instances): 
      pass  # should wait here, or do the actual start
    slave_instances = repair_slave_master_find_instances(instances_in_the_group, "SLAVE", StateCountDictIS)
    if slave_instances and ask_to_continue("Please start the SLAVE instances %s " % slave_instances):
      pass  # should wait here, or do the actual start

def cmd_check_mysql():
  ''' check the status of the mysql '''
  print "Verifying mysql status for cluster %s" % options.cluster_name
  ret = check_mysql_status()
  dbg_print("ret code = %s " % ret)
  if ret == RetCode.OK: print "Cluster mysql setup is OK"
  my_exit(ret)

def check_mysql_status():
  ''' check the status of the mysql '''
  #ret = sys_call("bash -x %s/validate_mysql_replication_setup.sh %s %s" % (this_file_dirname, options.zookeeper_connstr, options.cluster_name))
  ret = sys_call("bash %s/validate_mysql_replication_setup.sh %s %s" % (this_file_dirname, options.zookeeper_connstr, options.cluster_name))
  return ret

def cmd_get_relays_from_CM():
  ''' get relays hosts from relay cluster '''
  cluster_name = options.cluster_name
  rls = get_json_or_list_from_zookeeper("/%s/LIVEINSTANCES" % (cluster_name), getList=True)
  dbg_print(rls)
  relays_list = convert_zookeeper_list(rls)
  return relays_list
  
def cmd_get_random_relay():
  relays = cmd_get_relays_from_CM()
  for r in relays:
    dbg_print(r)
  import random
  idx = random.randrange(len(relays))
  print relays[idx] # pick a relay at random from the list
  my_exit(0)

def cmd_get_external_view():
  print "External View:"
  for db in ESPRESSO_DBS: 
    external_view = get_external_view(options.cluster_name, db)
    if not options.espresso_partitions: new_ev = external_view
    else:
      partitions = ["%s_%s" % (db, x) for x in options.espresso_partitions.split(",")]
      new_ev = {}
      for part in external_view:
        if part in partitions: new_ev[part] = external_view[part]
    print "Db %s EV = \n%s" % (db, json.dumps(new_ev, indent=2))

def cmd_unblock_zk():
  ''' unblock connection to zk from a relay '''
  if len(args) == 0:
    print "usage: espresso_doctor.py --command unblock_zk --cluster_name <CLUSTER_NAME> --zookeeper_connstr localhost:2181 [--dry_run] relayHost" 
    return
  relay = args[0] # relay's host
  (relayHost, relayPort) = relay.split('_')
  
  cmd = "bash %s/zk_block.sh unblock %s" % (this_file_dirname, relayHost)
  dbg_print("cmd=" + cmd)
  ret = 0
  if not options.dry_run:
    while (ret != 1):
    	ret = sys_call(cmd)
  	print "ret=" + str(ret)
  my_exit(ret)

def cmd_block_zk():
  ''' block connection to zk from a relay '''
  #get relay host
  if len(args) == 0:
    print "usage: espresso_doctor.py --command block_zk --cluster_name <CLUSTER_NAME> --zookeeper_connstr localhost:2181 [--dry_run] relayHost" 
    return
  relay = args[0] # relay's host
  (relayHost, relayPort) = relay.split('_')
  
  (zkHost, zkPort) = options.zookeeper_connstr.split(':')
  cmd = "bash %s/zk_block.sh block %s %s %s" % (this_file_dirname, zkHost, zkPort, relayHost)
  dbg_print("cmd=" + cmd)
  ret = 0
  if not options.dry_run:
    ret = sys_call(cmd)
  my_exit(ret)

def cmd_check_helix():
  ''' check the status of the cluster '''
  print "Verifying cluster state for cluster %s" % options.cluster_name
  ret = check_helix_status()
  dbg_print("ret code = %s " % ret)
  if ret == RetCode.OK: print "Cluster States are OK"
  else: print "ERROR in Cluster States"
  return ret
  #sys.exit(ret)
  #my_exit(ret)

def check_cluster_status_num_partition_match(db_name, ideal_state, external_view):
  # number of partitions should match
  ret = RetCode.OK
  if len(ideal_state.keys()) != len(external_view.keys()):
    my_warning("num of partitions in ideal state = %s does not match num of partitions in external view = %s" 
	% (len(ideal_state.keys()) , len(external_view.keys())))
    ret = RetCode.ERROR
  return ret 

def check_helix_status():
  ret = RetCode.OK
  if len(PartitionMismatch) > 0:
    my_warning("The following partitions are missing from external view: %s" % PartitionMismatch)
    ret = RetCode.ERROR
  if len(PartitionNoMaster) > 0:
    my_warning("The following partitions have no master: %s" % PartitionNoMaster)
    ret = RetCode.ERROR
  if len(PartitionWrongNumberofSlaves) > 0:
    my_warning("The following partitions does not have correct number of slaves (expected %s): %s" % (ReplicationFactor-1, PartitionWrongNumberofSlaves))
    ret = RetCode.ERROR
  if len(PartitionErrorState) > 0:
    my_warning("The following partitions have ERROR: %s" % (PartitionErrorState))
    ret = RetCode.ERROR
  return ret 

def analyze_cluster_status_check_instance_group(ErrorList):
  ''' returns the instance group instance1,instance2.. if the ErrorList has that paritition '''
  instance_error_group = []
  for item in ErrorList:
    db_partition = ":".join(item.split(":")[:2])
    for instance_group in InstancePartitionMapping:
      if db_partition in InstancePartitionMapping[instance_group] and instance_group not in instance_error_group:
        instance_error_group.append(instance_group)
  return instance_error_group

def analyze_cluster_status_count_state(db_name, partition_name, ev_or_is, state_count):
  if db_name not in state_count: state_count[db_name]={}
  for instance_name in ev_or_is[db_name][partition_name]:
    if instance_name not in state_count[db_name]:
      state_count[db_name][instance_name]={}
    state = ev_or_is[db_name][partition_name][instance_name]
    if state not in state_count[db_name][instance_name]:
      state_count[db_name][instance_name][state] = 1
    else:
     state_count[db_name][instance_name][state] +=1

def analyze_cluster_status_refresh_ev_is():
  global ExternalViews, IdealStates, DatabasePartitionMapping, StateCountDictEV, StateCountDictIS, InstancePartitionMapping
  global PartitionMismatch, PartitionNoMaster, PartitionWrongNumberofSlave, PartitionErrorState, ReplicationFactor

  live_instances = get_live_instances(options.cluster_name)
  db_names = get_db_names(options.cluster_name)
  for db_name in db_names:
    ideal_state = get_ideal_state(options.cluster_name, db_name)
    IdealStates[db_name] = ideal_state
    DatabasePartitionMapping[db_name]=ideal_state.keys()
    external_view = get_external_view(options.cluster_name, db_name)
    ExternalViews[db_name] = external_view
    first_partition = ideal_state.keys()[0]
    ReplicationFactor = min(len(ideal_state[first_partition].keys()),len(live_instances))  # Replication Factor less than number of instances

def analyze_cluster_status():
  global ExternalViews, IdealStates, DatabasePartitionMapping, StateCountDictEV, StateCountDictIS, InstancePartitionMapping
  global PartitionMismatch, PartitionNoMaster, PartitionWrongNumberofSlave, PartitionErrorState, ReplicationFactor

  analyze_cluster_status_refresh_ev_is()

  db_names = DatabasePartitionMapping.keys()
  for db_name in db_names:
    StateCountDictEV[db_name]={}
    for partition_name in IdealStates[db_name]:
      key = ",".join(sorted(IdealStates[db_name][partition_name].keys()))
      if key not in InstancePartitionMapping: InstancePartitionMapping[key]=[]
      if not partition_name in ExternalViews[db_name]:
        PartitionMismatch.append("%s:%s" % (db_name, partition_name))
      else:
        InstancePartitionMapping[key].append("%s:%s" % (db_name, partition_name))    # partitions for that instance group

      if partition_name not in ExternalViews[db_name]: continue    # all the partitions are down
      num_master = len([x for x in ExternalViews[db_name][partition_name].values() if x=="MASTER"])
      num_slave = len([x for x in ExternalViews[db_name][partition_name].values() if x=="SLAVE"])
      num_error = len([x for x in ExternalViews[db_name][partition_name].values() if x=="ERROR"])
      if num_master !=1: PartitionNoMaster.append("%s:%s:num=%s" % (db_name, partition_name, num_master))
      if num_slave !=ReplicationFactor-1: PartitionWrongNumberofSlaves.append("%s:%s:num=%s" % (db_name, partition_name, num_slave))
      if num_error !=0: PartitionErrorState.append("%s:%s:num=%s" % (db_name, partition_name, num_error))
      analyze_cluster_status_count_state(db_name, partition_name, ExternalViews, StateCountDictEV)
      analyze_cluster_status_count_state(db_name, partition_name, IdealStates, StateCountDictIS)

  # find out the instance group needs to be repaired
  global InstanceErrorGroups
  InstanceErrorGroups = analyze_cluster_status_check_instance_group(PartitionMismatch + PartitionNoMaster + PartitionWrongNumberofSlaves + PartitionErrorState)

  #print "DatabasePartitionMapping = %s " %  pprint.pformat(DatabasePartitionMapping)
  print "StateCountDictEV = \n%s" % json.dumps(StateCountDictEV, indent=2)
  #print "StateCountDictEV = \n%s" % pprint.pformat(json.dumps(StateCountDictEV))
  #print "StateCountDictIS = \n%s" % pprint.pformat(StateCountDictIS)
  #print "InstancePartitionMapping = \n%s" % pprint.pformat(InstancePartitionMapping)
  print "InstanceErrorGroups = %s" % pprint.pformat(InstanceErrorGroups)
    #dbg_print("status = %s" % pprint.pformat(status))
  #print "InstancePartitionMapping = %s " % InstancePartitionMapping
      
def get_valid_cmds(cmd_type="cmd"):
  return [x.split("%s_" % cmd_type)[1] for x in globals() if callable(globals()[x]) and re.search("^%s_" % cmd_type,x) ]

def dispatch_command(cmd):
  cmd_func_name = "cmd_%s" % cmd
  if cmd_func_name not in globals(): 
    my_error("cmd '%s' is not a valid command. Valid commands are %s\n line = %s\n " % (cmd, get_valid_cmds(), line))
  ret = globals()[cmd_func_name]()
  return ret

def setup_schemas_registry_dir():
  global ESPRESSO_SCHEMA_REGISTRY_DIR
  ESPRESSO_SCHEMA_REGISTRY_DIR
  dbname = ESPRESSO_DBS[0]   # get one db
  schemata_dir=os.path.join(ESPRESSO_SCHEMA_REGISTRY_DIR,"schemata/document/%s" % dbname)
  table_list= get_json_or_list_from_zookeeper("/%s/schemata/document/%s" % (ESPRESSO_SCHEMA_REGISTRY_DIR,dbname), getList=True)
  if not table_list: 
    table_list= get_json_or_list_from_zookeeper("/%s/%s/schemata/document/%s" % (ESPRESSO_SCHEMA_REGISTRY_DIR, options.cluster_name ,dbname), getList=True)
    ESPRESSO_SCHEMA_REGISTRY_DIR="%s/%s" % (ESPRESSO_SCHEMA_REGISTRY_DIR,options.cluster_name)
  dbg_print("table_list = %s" % table_list)
  if not table_list: my_error("Cannot get table list from zookeeper for db %s" % dbname) 

def setup_stoage_node_info():
  global MASTER_MYSQL_PORTS, ESPRESSO_STORAGE_NODE_ADDRS, MASTER_INDEX_MYSQL_PORTS, MASTER_INDEX_FILE_PATH
  ESPRESSO_STORAGE_NODE_ADDRS=get_storage_nodes(options.cluster_name)
  for storage_node_addr in ESPRESSO_STORAGE_NODE_ADDRS:
    #storage_node_index = ESPRESSO_STORAGE_NODE_ADDRS.index(storage_node_addr)
    MASTER_MYSQL_PORTS.append(get_mysql_port(options.cluster_name,storage_node_addr))
    MASTER_INDEX_MYSQL_PORTS.append(get_index_mysql_port(options.cluster_name,storage_node_addr))
    MASTER_INDEX_FILE_PATH.append(get_index_file_path(options.cluster_name,storage_node_addr))


saved_zookeeper_connstr=None
def push_zk_connstr(zk_connstr):
  global saved_zookeeper_connstr
  saved_zookeeper_connstr = get_zookeeper_conn_str()
  set_zookeeper_conn_str(zk_connstr)
def pop_zk_connstr():
  set_zookeeper_conn_str(saved_zookeeper_connstr)

def setup_dr_stoage_node_info():
  global DR_MASTER_MYSQL_PORTS, DR_ESPRESSO_STORAGE_NODE_ADDRS, DR_MASTER_INDEX_MYSQL_PORTS, DR_MASTER_INDEX_FILE_PATH
  push_zk_connstr(options.dr_zookeeper_connstr)
  DR_ESPRESSO_STORAGE_NODE_ADDRS=get_storage_nodes(options.dr_cluster_name)
  for storage_node_addr in DR_ESPRESSO_STORAGE_NODE_ADDRS:
    #storage_node_index = DR_ESPRESSO_STORAGE_NODE_ADDRS.index(storage_node_addr)
    DR_MASTER_MYSQL_PORTS.append(get_mysql_port(options.dr_cluster_name,storage_node_addr))
    DR_MASTER_INDEX_MYSQL_PORTS.append(get_index_mysql_port(options.dr_cluster_name,storage_node_addr))
    DR_MASTER_INDEX_FILE_PATH.append(get_index_file_path(options.dr_cluster_name,storage_node_addr))
  pop_zk_connstr()
    
def setup_env(): 
  global ESPRESSO_NUM_PARTITIONS, ESPRESSO_DBS, ESPRESSO_TABLES, ROW_COUNT_TABLES  
  ESPRESSO_DBS=get_db_names(options.cluster_name)
  dbg_print("ESPRESSO_DBS= %s" % ESPRESSO_DBS)
  setup_schemas_registry_dir()
  setup_stoage_node_info()
  if options.compare_with_dr: setup_dr_stoage_node_info()
  for dbname in ESPRESSO_DBS:
    ESPRESSO_NUM_PARTITIONS[dbname] = get_number_of_partitions(dbname)
    ESPRESSO_TABLES[dbname] = get_espresso_tables(dbname)
  if options.espresso_table_names: ROW_COUNT_TABLES = options.espresso_table_names.split(",")
  if options.espresso_db_names: ESPRESSO_DBS = options.espresso_db_names.split(",")

def main(argv):
  global options
  global args
  parser = OptionParser(usage="usage: %prog [options]")
  parser.add_option("--helix_host", action="store", dest="helix_host", default="localhost", help="Host of the helix webapp [default: %default]")
  parser.add_option("-p", "--helix_port", action="store", dest="helix_port", default=12925,  type="int", help="Port of the helix webapp [default: %default]")
  #parser.add_option("--helix_admin_port", action="store", dest="helix_admin_port", default=12929,  type="int", help="Admin Port of the helix webapp [default: %default]")
  parser.add_option("--cluster_name", action="store", dest="cluster_name", default="ESPRESSO_STORAGE", help="cluster name [default: %default]")
  parser.add_option("--relay_cluster_name", action="store", dest="relay_cluster_name", default=None, help="relay cluster name [default: %default]")
  parser.add_option("--dbus_client_addr", action="store", dest="dbus_client_addr", default=None, help="host:port of databus client [default: %default]")
  parser.add_option("--subs", action="store", dest="subs", default=None, help="databases client subscribes to [default: %default]")
  parser.add_option("--zookeeper_connstr", action="store", dest="zookeeper_connstr", default="localhost:2181", help="zookeeper connection str [default: %default]")
  parser.add_option("-c", "--command", action="store", dest="command", default="check_helix", choices=get_valid_cmds(), help="Possible commands %s" % get_valid_cmds())
  parser.add_option("--state_to_list", action="store", dest="state_to_list", default=None, choices=PossiblePartitionState, help="The state to list in list partition. Possible values are  %s" % PossiblePartitionState)
  parser.add_option("", "--force_repair", action="store_true", dest="force_repair", default = True, help="call the repair tool forcefully")
  parser.add_option("", "--espresso_db_names", action="store", dest="espresso_db_names", default="",
                     help="db names to select row count. [default: all the db]")
  parser.add_option("", "--espresso_table_names", action="store", dest="espresso_table_names", default="",
                     help="table tables to select row count. [default: all the tables]")
  parser.add_option("", "--espresso_partitions", action="store", dest="espresso_partitions", default="",
                     help="Espresso partitions to show external views. example 0,1,2 [default: all the partitions]")
  parser.add_option("", "--thread_pool_size", action="store", type="int", dest="thread_pool_size", default = 5,
                     help="Thread pool size to query mysql. [default: %default]")
  parser.add_option("", "--use_lucene_mysql", action="store_true", dest="use_lucene_mysql", default = False, 
                     help="Whether to use lucene mysql or not [Default: %default]")
  parser.add_option("", "--use_lucene_filesystem", action="store_true", dest="use_lucene_filesystem", default = False, 
                     help="Whether to use lucene filesystem or not [Default: %default]")
  parser.add_option("", "--retryInterval", action="store", type="int", dest="retryInterval", default = 30,
                     help="Interval to retry [default: %default]")
  parser.add_option("", "--retryTimeout", action="store", type="int", dest="retryTimeout", default = None,
                     help="retry Time Out [default: None. No retry]")
  parser.add_option("", "--row_count_use_ideal_state", action="store_true", dest="row_count_use_ideal_state", default = False, help="[default: %default]")
  parser.add_option("", "--expect_zero", action="store_true", dest="expect_zero", default=False, help="Whether to expect all db/tables to have 0 rows or indices. [Default: %default]")
  parser.add_option("", "--check_master_only", action="store_true", dest="check_master_only", default=False, help="Whether to verify if slave partition HWM matches relay. [Default: %default]")
  parser.add_option("", "--compare_with_dr", action="store_true", dest="compare_with_dr", default=False, help="If set, need to compare with the DR cluster. [Default: %default]")
  parser.add_option("", "--dr_cluster_name", action="store", dest="dr_cluster_name", default="ESPRESSO_STORAGE", help="DR cluster name. [Default: %default]")
  parser.add_option("", "--dr_zookeeper_connstr", action="store", dest="dr_zookeeper_connstr", default=None, help="DR zookeeper connstr. [Default: %default]")

  debug_group = OptionGroup(parser, "Debug options", "")
  debug_group.add_option("-v", "--verbose", action="store_true", dest="verbose", default = False, help="verbose mode")
  debug_group.add_option("", "--dry_run", action="store_true", dest="dry_run", default = False, help="dry run mode")
  debug_group.add_option("-d", "--debug", action="store_true", dest="debug", default = False,
                     help="debug mode")
  debug_group.add_option("", "--sys_call_debug", action="store_true", dest="enable_sys_call_debug", default = False,
                     help="debug sys call")
  parser.add_option_group(debug_group)
  (options, args) = parser.parse_args()

  if options.relay_cluster_name==None:
    options.relay_cluster_name = options.cluster_name.replace("ESPRESSO_","RELAY_")  #most FT clusters use this nomenclature, so assume that by default
  if options.dr_zookeeper_connstr==None:
    options.dr_zookeeper_connstr=options.zookeeper_connstr

  if "SCRIPT_DEBUG" in os.environ and os.environ["SCRIPT_DEBUG"].lower() == "true": options.debug = True
  set_debug(options.debug)
  set_sys_call_debug(options.enable_sys_call_debug)
  dbg_print("options = %s  args = %s" % (options, args))

  # always user zookeeper
  global UseZooKeeperForExternalView
  UseZooKeeperForExternalView = True
  #zookeeper_cli_setup()
  set_zookeeper_conn_str(options.zookeeper_connstr)
  if options.command == "check_row_count" or options.command == "waitfor_row_count":
    setup_env()
    get_row_count_start_thread_pool()
  if options.command == "check_index_count" or options.command == "waitfor_index_count":
    setup_env()
    get_index_count_start_thread_pool()
  if options.command == "check_hwm_relay" or options.command == "waitfor_hwm_relay":
    setup_env()
    get_hwm_start_thread_pool()
  if options.command == "check_hwm_client" or options.command == "waitfor_hwm_client":
    setup_env()
    get_hwm_start_thread_pool()
  if options.command =="get_external_view" or options.command == "run_consistency_check":
    setup_env()

  '''
  if not isOpen(options.helix_host, options.helix_port):
    my_warning("%s:%s is not accepting connection. Use zookeeper" % (options.helix_host, options.helix_port))
    global UseZooKeeperForExternalView
    UseZooKeeperForExternalView = True
    zookeeper_cli_setup()
  else: 
    # check clusters
    global helix_url_cluster, helix_url_clusters
    helix_url_clusters="http://%s:%s/clusters" % (options.helix_host, options.helix_port)
    clusters= get_clusters(helix_url_clusters)
    if options.cluster_name not in clusters:
      my_error("Given cluster name %s is not a valid cluster. Valid clusters are %s" % (options.cluster_name, clusters))
    helix_url_cluster="%s/%s/resourceGroups" % (helix_url_clusters, options.cluster_name)
  '''


  if options.command not in  ['block_zk','unblock_zk','get_external_view']:
    analyze_cluster_status()

  ret = dispatch_command(options.command)

  my_exit(ret)
  
if __name__ == "__main__":
  main(sys.argv[1:])


