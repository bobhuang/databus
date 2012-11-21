#!/usr/bin/env python
'''
  Helper to issue admin call for espresso backup restore

   
'''
__author__ = "David Zhang"
__version__ = "$Revision: 0.1 $"
__date__ = "$Date: 2012/03/01 $"
import sys, os
import pdb
import re
from optparse import OptionParser, OptionGroup
from utility import *
import httplib
import pprint
import json, urllib2
import datetime
        
# Global constant
PossibleCommands=["BackupNow","BackupNext","BackupIndexNow","Restore","RestoreNow","RestoreNext","ArchiveNow","ArchiveIndexNow","ArchiveNext","RestoreIndex","RestoreIndexNext","RestoreIndexNow","BackupIndexNext","GetZKSCN","MySqlChecksumGenerator","StartBackupScheduler","StopBackupScheduler","StartArchiveScheduler","StopArchiveScheduler"]
PossiblePartitionState=["MASTER","SLAVE","BLANK"]

# Global variables
options=None

def construct_criteria():
  criteria= {
#    "instanceName": options.instance_name,
#    "resource": options.dbname,
#    "partition": "%s_%s" % (options.dbname, options.partition_id),
#    "partitionState": options.partition_state,
    "recipientInstanceType": "PARTICIPANT", 
    "selfExcluded": "false",
    "sessionSpecific": "true",
    "dataSource": "EXTERNALVIEW", 
  }
  #if options.command != "Restore" and options.command != "RestoreIndex":  # restore do not need the dbname and partition in critieria
  if options.dbname:  criteria["resource"]=options.dbname
    # add this criteria only if command is Backup
  if "Backup" in options.command: 
    if options.dbname and options.partition_id is not None: criteria["partition"]="%s_%s" % (options.dbname, options.partition_id)
  if options.instance_name: criteria["instanceName"]=options.instance_name
  if options.partition_state:  criteria["partitionState"]=options.partition_state
 
# % (options.instance_name, options.dbname, "%s_%s" % (options.dbname, options.partition_id), options.partition_state)
  return criteria

def get_msg_type(cmd):
  m = re.search("^(BackupIndex|Backup|ArchiveIndex|Archive|RestoreIndex|Restore)(\w+)", cmd)
  if m:
   if "Index" in m.group(2): 
     return cmd
   else:
     return m.group(1) # for BackupNow and BackupNext
  m = re.search("^(Start|Stop)(Backup|Restore|Archive)(Scheduler)$", cmd)
  if m: return "startStopScheduler"
  return options.command

def get_sub_msg_type(cmd):
  m = re.search("^(Restore|RestoreIndex)(.*)", cmd)
 
  if m: return "scheduleNow"

  m = re.search("^(MySqlChecksumGenerator)", cmd)
  if m: return "scheduleNext"

  m = re.search("^(BackupIndex|Backup|ArchiveIndex|Archive)(\w+)", cmd)
  if m: return "schedule" + m.group(2)

  m = re.search("^(Start|Stop)(Backup|Restore|Archive)(Scheduler)$", cmd)
  if m: return m.group(1).replace('S','s',1) + "Scheduler"

  else: return None

def construct_message_template(sub_msg_type=None):
  msg_type = get_msg_type(options.command)
  print "*********************%s*************" % msg_type
  if not sub_msg_type: sub_msg_type = get_sub_msg_type(options.command)
  message_template = {
      "MSG_ID": "Template", 
      "MSG_STATE": "new", 
      "MSG_TYPE": msg_type,
      "TGT_SESSION_ID": "*"
  } 
  if sub_msg_type: message_template["MSG_SUBTYPE"]=sub_msg_type
  print "*********************%s*************" % sub_msg_type
  if options.dbname and options.partition_id is not None:
    if re.search("^Restore",options.command):
      message_template["BOOTSTRAP_PARTITION"]="%s_%s" % (options.dbname, options.partition_id)
    if options.command == "ArchiveNow" or options.command == "ArchiveIndexNow":     
      message_template["ARCHIVE_PARTITION"]="%s_%s" % (options.dbname, options.partition_id)
  if options.tablename is not None:
    message_template["tableName"]=options.tablename
  if options.file_path is not None:
    message_template["USE_BKP_FILE"]=options.file_path
  m = re.search("^(Start|Stop)(Backup|Restore|Archive)(Scheduler)$", options.command)
  if m:
    message_template["schedulerName"]=m.group(2)
    if m.group(1) == "Start":
      if options.cron: 
        message_template["setCron"]=options.cron
        message_template["setTimezone"]="PST"
    
  return message_template

def send_call_encode(d, join_char="&", equal_char="="):
  ''' give a dictionary, return key:val '''
  key_val = []
  for key in d:
    key_val.append("%s%s%s" % (key,equal_char, d[key]))
  return join_char.join(key_val)
    
def send_call(criteria,msg_template):
  import httplib
  conn = httplib.HTTPConnection(options.host, options.port)
  if options.debug: conn.set_debuglevel(1)
  uri = ("clusters/%s/SchedulerTasks" % (options.cluster_name))
  request_dict = {'Criteria': json.dumps(criteria), 'MessageTemplate': json.dumps(msg_template)}
  body = urllib.urlencode(request_dict)
  headers = {"Content-type": "application/json"}
  print ("curl -d '%s' -H '%s' http://%s:%s/%s" % (send_call_encode(request_dict), send_call_encode(headers,'&',':'), options.host, options.port, uri))
  #dbg_print("body = %s" % body)
  conn.request("POST",uri, body, headers)
  response = conn.getresponse()
  #dbg_print ("response.status = %s \n response.reason= %s " % (response.status, response.reason))
  data = response.read()
  #dbg_print (data)
  conn.close()
  return data

def wait_for_task_completion(criteria):
  msg_template = construct_message_template("getStatus")
  
  def task_completed():
    ret_data = send_call(criteria,msg_template)
    log_info("Time BR operation started::::%s" % datetime.datetime.now()) 
    wait_for_message_completion(ret_data)   # first wait for the message to finish, then pull the url
    status = get_status(ret_data)
    dbg_print("status = %s" % pprint.pformat(status))
    if not status: 
      my_warning("No: result for getStatus call")
      return False
    result_key = None
    try:
     for key in status["mapFields"]:
      if re.search("MessageResult.*",key):
        result_key = key
        break
     if not result_key:
      my_warning("No: result key in getStatus result")
      return False
     if (status["mapFields"][result_key]["StatusCode"]=="ERROR"):
	log_info("*********** Got error, exiting :***************:%s" % status["mapFields"][result_key]["ERRORINFO"]);
        my_exit(status["mapFields"][result_key]["ERRORINFO"]);
     #log_info("*********** No error, Normal exit :***************");
     return status["mapFields"][result_key]["StatusCode"]=="COMPLETE"
    except: return False
  ret = wait_for_condition_1(task_completed, options.timeout, sleep_interval=8)
  if ret == RetCode.TIMEOUT: my_warning("wait_for_task_completion timed out")
  return ret

def get_status(ret_data):
  ''' get the status data from ret_data which will have the url '''
  ret_json = json.loads(ret_data)
  if ret_json:
    status_url2 = ret_json["statusUpdateUrl"]
    status_url = status_url2.replace(":null",":12929",1) #working around a known bug which is fixed in subsequent versions but not pushed into FT1 cluster
  else: 
    my_warning("Not status update url in response")
    return None
  #dbg_print("status_url = %s" % status_url)
  try:
    time.sleep(2)
    sf = urllib2.urlopen(status_url)
    status = json.loads(sf.read())
    sf.close()
  except IOError as (errno, strerror):
    print "I/O error({0}): {1}".format(errno, strerror)
    return None
  except:
    print "Unexpected error:", sys.exc_info()[0]
    raise
  if not status: 
    my_warning("Did not get any status")
    return None
  #log_info("status************ = %s" % status)
  return status

def get_message_error(data):
   ''' get if ret_data has mapFields[MessageResult][ERRORINFO] '''
   #log_info("*********** Entering get_message_error:***************:%s"% data);
   result_key=""
   if (data and "mapFields" in data):
     for key in data["mapFields"]:
      if re.search("MessageResult.*",key):
       result_key = key
       break
     if (len(result_key)==0):
       my_warning("No MessageResult key in ret_data")
       return False
     log_info("result_key = %s" % result_key)
     if (data["mapFields"][result_key]):
      for key_result in data["mapFields"][result_key]:
       if("ERRORINFO" in key_result):
        log_info("*********** Got msg error, exiting :***************:%s"% data["mapFields"][result_key]["ERRORINFO"]);
        my_exit(data["mapFields"][result_key]["ERRORINFO"]);
   log_info("%s:Task completed with No error, Normal exit from message" % datetime.datetime.now());
   return True

def wait_for_message_completion(ret_data):
  ''' this is when the message completes and tasks started '''
  def message_completed():
    # check if ret_data has mapFields[MessageResult][ERRORINFO] 
    status = get_status(ret_data)   
    dbg_print("status = %s" % pprint.pformat(status))
    if not status: 
      #dbg_log_info("status = %s " % status)
      return False
    msgerror = get_message_error(status)
    if(status and "mapFields" in status):
     for k,v in status["mapFields"].iteritems():
      #if "AdditionalInfo" in v and v["AdditionalInfo"] == "Message handling task completed successfully":
      #if "AdditionalInfo" in v:
      #log_info("######Message handling task completed AdditionalInfo= %s " %  v["AdditionalInfo"])
      if "AdditionalInfo" in v and v["AdditionalInfo"] == "Scheduler task completed":
	return True
    return False
  ret = wait_for_condition_1(message_completed, timeout=120, sleep_interval=5)
  if ret == RetCode.TIMEOUT: my_warning("wait_for_message_completion timed out after 120 secs")
  return ret

def get_gen_id_sql(sql_cnf_port):
  ''' get the current gen id from sql '''
  port1= sql_cnf_port
  retval = -1
  qry = "select get_binlog_gen_id(\"es_%s_%s\");" % (options.dbname,options.partition_id)
  seq_sql = mysql_exec_sql_one_row(qry,dbname=None, user="espresso", passwd="espresso", host="localhost", port=port1)
  if(seq_sql is not None and seq_sql[0] is not None and seq_sql[0]):
    retval=int(seq_sql[0])
  return retval

def get_seq_id_sql(sql_cnf_port):
  ''' get the current seq id from sql '''
  port1= sql_cnf_port
  retval = -1
  qry = "select get_binlog_seq_id(\"es_%s_%s\");" % (options.dbname,options.partition_id)
  seq_sql = mysql_exec_sql_one_row(qry,dbname=None, user="espresso", passwd="espresso", host="localhost", port=port1)
  if(seq_sql is not None and seq_sql[0] is not None and seq_sql[0]):
    retval=int(seq_sql[0])	
  return retval

def get_table_checksum_sql(host,sql_cnf_port, tabname):
  ''' get the current gen id from sql '''
  if("Index" in options.command):
    prefix="esi"
  else:
    prefix="es"
  port1= sql_cnf_port
  retval = 0
  qry = "use %s_%s_%s;checksum table %s;" % (prefix,options.dbname, options.partition_id, tabname)
  seq_sql = mysql_exec_sql_one_row(qry,dbname=None, user="espresso", passwd="espresso", host=host, port=port1)
  print " checksum:%s" % seq_sql[1] # col[0] is name of table and col[1] is the checksum
  if(seq_sql is not None and seq_sql[1] is not None and seq_sql[1]):
    retval=int(seq_sql[1])
  return retval

def get_table_row_count(host,sql_cnf_port,tabname):
  ''' get the current gen id from sql '''
  retval = 0
  port1= sql_cnf_port
  cmd="mysql -h %s -uespresso -pespresso --protocol tcp --port %s -e \"select count(*) from es_%s_%s.%s;\"" %(host,port1,options.dbname,options.partition_id,tabname)
  print "row cmd is %s" % cmd
  retval= sys_pipe_call(cmd)
  print ("row count is %s" % retval)
  return retval



def get_zk_backupPath(host, port, backuptype):    
  cmJson = get_zk_info(options.cluster_name,options.zkSvr,options.dbname, options.partition_id,host,backuptype)
  if(cmJson is not None and (len(cmJson)> 0) ):
   print" Last element in cmjson:%s" % cmJson[len(cmJson)-1]["backupFilePath"]	
   key = cmJson[len(cmJson)-1]
   log_info("***********backupFilePath:%s***************:" % key["backupFilePath"])
   if(key["backupFilePath"]):
      backupPath = key["backupFilePath"]
      backupdir=backupPath[0:backupPath.rfind("/")]	  
      print "backup file path %s" % backupdir
      # if Archive is successful , only 2 files are left over in the above path
      num_files=len([name for name in os.listdir(backupdir) if os.path.isfile(backupdir+"/"+name)])
      print num_files
      if(num_files != 2):
	 log_info("Archive validation FAILED")
         my_exit(1)
      else: 
         log_info("Archive validation PASSED")         	
   return num_files 

def validate_restore():
     #validate seq from binlog in backup and restore db
     if("TestDB" in options.dbname ): # cnf port for Backup and instance name contains localhost and db contains TestDB- only for CI tests
         log_info("getting mysql port for backup and restore partitions")
         if("Index" in options.command):
	    tabname="Table2_index_norpl_esp"
            print "Doing Index Restore verification with %s" % tabname
         else:
            tabname="Table2"
	 sql_cnf_port_backup=get_mysql_port("localhost_12932",options.cluster_name,options.zkSvr)   #cnf port for Backup instance
         sql_cnf_port_restore=get_mysql_port("localhost_12930",options.cluster_name,options.zkSvr)   #cnf port for Restore
         sql_ret_seq_backup = get_seq_id_sql(sql_cnf_port_backup)
         sql_ret_gen_backup = get_gen_id_sql(sql_cnf_port_backup)
         sql_ret_checksum_backup = get_table_checksum_sql("localhost",sql_cnf_port_backup,tabname)
         sql_ret_tabrowcount_backup = get_table_row_count("localhost",sql_cnf_port_backup,tabname)
         sql_ret_seq_restore = get_seq_id_sql(sql_cnf_port_restore)
         sql_ret_gen_restore = get_gen_id_sql(sql_cnf_port_restore)
         sql_ret_checksum_restore = get_table_checksum_sql("localhost",sql_cnf_port_restore,tabname)
         sql_ret_tabrowcount_restore = get_table_row_count("localhost",sql_cnf_port_restore,tabname)
         if(sql_ret_gen_backup != sql_ret_gen_restore):
            print("FAIL : Restore generations dont match, backup has %s, restore has %s" %(sql_ret_gen_backup,sql_ret_gen_restore))
            my_exit(1)
         if(sql_ret_seq_backup != sql_ret_seq_restore):
            print("FAIL :Restore seq id dont match, backup has %s, restore has %s" %(sql_ret_seq_backup,sql_ret_seq_restore))
            my_exit(1)
         print("Gen and seq matched after restore Genid:%s and seqis:%s" % (sql_ret_gen_restore,sql_ret_seq_restore)) 
         if(sql_ret_checksum_backup != sql_ret_checksum_restore):
            print("FAIL :Restore checksum on Table1 doesnt match, backup has %s, restore has %s" %(sql_ret_checksum_backup,sql_ret_checksum_restore))
            my_exit(1)
         print("Checksum matched after restore checksum:%s" % (sql_ret_checksum_backup)) 
         if(sql_ret_tabrowcount_backup != sql_ret_tabrowcount_restore):
            print("FAIL :Restore table row count on Table1 doesnt match, backup has %s, restore has %s" %(sql_ret_tabrowcount_backup,sql_ret_tabrowcount_restore))
            my_exit(1)
         print("table row count matched after restore :%s" % (sql_ret_tabrowcount_restore)) 
         return

def get_zk_backupUri_sequence(host, port, backuptype):    
     cmJson = get_zk_info(options.cluster_name,options.zkSvr,options.dbname, options.partition_id,host,backuptype)
     #validate seq anyway comparing from filename and binlog 
     if(options.partition_id in [0,2] and "TestDB" in options.dbname ): # cnf port for Backup and instance name contains localhost and db contains TestDB- only for CI tests
         log_info("PICKING PORT:15001")
	 sql_cnf_port="15001"    #cnf port for Backup
     time.sleep(5)
     sql_ret_seq = get_seq_id_sql(sql_cnf_port)
     sql_ret_gen = get_gen_id_sql(sql_cnf_port)
     print "checksum is :%s" % get_table_checksum_sql(host,sql_cnf_port,"Table1")
     print "in found options.validate %s" % cmJson
     if(options.validate is not None and "=" in options.validate): #this options has atleast one pair
         for pair in options.validate.split(";"):
	  print " option[i]:%s" % pair
          (k, v) = pair.split("=")
          print " validate_options:%s has value %s" % (k,v)

          if ("numzk" in k and "Backup" in options.command):
             print "****** validating num of zk entries %s *****" % v
             log_info("***********No of records in ZK for this operation:%s***************:" % len(cmJson))  #returns number of ZK records
             if(len(v)>0 and int(v,10)==len(cmJson)):
	       log_info("zk validation PASS")
               return
             else: 
               log_info("zk validation FAILED")
               my_exit(1)
     if(cmJson is not None and (len(cmJson)> 0) ):
       print" Last element in cmjson:%s" % cmJson[len(cmJson)-1]["backupFilePath"]	
       key = cmJson[len(cmJson)-1]
       log_info("***********backupFilePath:%s***************:" % key["backupFilePath"])
       # if file exists - get the seq and gen from it and compare below
       try:
   	  with open(key["backupFilePath"]) as f: 
            print "backup file exists - validation Passed %s" % key["backupFilePath"]
       except IOError as e:
          log_info("backup file does not exist - validation FAILED")
          my_exit(1)
       log_info("***********zk and file sequence:%s***************:" % key["backupSCN"]["sequence"])
       log_info("***********zk and file generation:%s***************:" % key["backupSCN"]["generation"])

       if (int(key["backupSCN"]["sequence"])==sql_ret_seq):
          log_info("sequence validation PASS")
       else: 
          print "****** Test is expecting sequence %s *****" % sql_ret_seq
          log_info("seq validation FAILED")
          my_exit(1)
       if (int(key["backupSCN"]["generation"])==sql_ret_gen):
          log_info("generation validation PASS")
       else: 
          print "****** Test is expecting sequence %s *****" % sql_ret_gen
          log_info("generation validation FAILED")
          my_exit(1)
       return


def main(argv):
  global options
  parser = OptionParser(usage="usage: %prog [options]")
  parser.add_option("--host", action="store", dest="host", default="localhost", help="Host of the helix webapp [default: %default]")
  parser.add_option("-p", "--port", action="store", dest="port", default=12925,  type="int", help="Port of the helix webapp [default: %default]")
  parser.add_option("--helix_webapp_host_port", action="store", dest="helix_webapp_host_port", default=None, help="colon separated Host:Port of the helix webapp. Same as using host port separately. Just make it easier to call from other script. This one will override the host:port  [default: %default]")
  parser.add_option("--cluster_name", action="store", dest="cluster_name", default="ESPRESSO_STORAGE", help="cluster name [default: %default]")
  parser.add_option("--instance_name", action="store", dest="instance_name", default=None, help="instance name [default: %default]")
  parser.add_option("-c", "--command", action="store", dest="command", default=None, choices=PossibleCommands, help="Possible commands %s" % PossibleCommands)
  parser.add_option("--dbname", action="store", dest="dbname", default=None, help="db name [default: %default]")
  parser.add_option("--tablename", action="store", dest="tablename", default=None, help="table name [default: %default]")
  parser.add_option("--partition_id", action="store", dest="partition_id", default=None, type="int", help="[default: %default]")
  parser.add_option("--zkSvr", action="store", dest="zkSvr", default="localhost:2181", help="zk connection string eg localhost:2181")
  parser.add_option("--partition_state", action="store", dest="partition_state", default=None, choices=PossiblePartitionState, help="Possible commands %s" % PossiblePartitionState)
  parser.add_option("-f", "--file_path", action="store", dest="file_path", default=None, help="file path of the file to restore from. If not given, Retore will find out from zk [default: %default]")
  parser.add_option("--cron", action="store", dest="cron", default=None, help="cron expression. If not given, just start scheduler w/o setting cron [default: %default]")
  
  OpCompletion_group = OptionGroup(parser, "Wait for operation Completion options", "")
  OpCompletion_group.add_option("-w" ,"--wait_for_completion", action="store_true", dest="wait_for_completion", default = False, help="if set, wait for completion")
  OpCompletion_group.add_option("--wait_for_message_completion", action="store_true", dest="wait_for_message_completion", default = False, help="if set, wait for completion of the initial message")
  OpCompletion_group.add_option("" ,"--timeout", action="store", dest="timeout", default=30, help="set this timeout if different from 120s")
  parser.add_option_group(OpCompletion_group)

  ValidateTest_group = OptionGroup(parser, "Validate operation like check scn, generation, sequence", "")
  ValidateTest_group.add_option("-v" ,"--validate", action="store", dest="validate", default = None, help="if set, validate:\"sequence=5;numzk=2\"")
  parser.add_option_group(ValidateTest_group)
 
  debug_group = OptionGroup(parser, "Debug options", "")
  debug_group.add_option("-d", "--debug", action="store_true", dest="debug", default = False,
                     help="debug mode")
  debug_group.add_option("", "--sys_call_debug", action="store_true", dest="enable_sys_call_debug", default = False,
                     help="debug sys call")
  parser.add_option_group(debug_group)

  (options, args) = parser.parse_args()
  if "SCRIPT_DEBUG" in os.environ and os.environ["SCRIPT_DEBUG"].lower() == "true": options.debug = True
  set_debug(options.debug)
  set_sys_call_debug(options.enable_sys_call_debug)
  #dbg_print("options = %s  args = %s" % (options, args))
  if options.helix_webapp_host_port:
     (options.host, options.port) = tuple(options.helix_webapp_host_port.split(":"))

  criteria = construct_criteria()
  log_info("criteria = %s" % criteria)
  msg_template = construct_message_template()
  log_info("msg_template = %s" % msg_template)
  ret_data = send_call(criteria,msg_template)
  print ("================send_call ret status:%s==================" % ret_data)
  if options.wait_for_completion or options.wait_for_message_completion: 
    ret = wait_for_message_completion(ret_data)
    if ret != RetCode.OK: my_exit(ret)
  if options.wait_for_completion: 
    ret = wait_for_task_completion(criteria)
    if ret != RetCode.OK: my_exit(ret)
    if options.validate: 
     log_info("***********options.validate:%s_%s*:" % (options.validate,options.command))
     if("Index" in options.command):
        filegenerated = "Indexes"
     else:	
         filegenerated = "MySQL"
     if("Archive" in options.command):
      log_info("***********options.validate:%s" % (options.command))
      get_zk_backupPath(options.host, options.port, filegenerated) 
     if("Backup" in options.command):
      log_info("***********options.validate:%s_%s*:" % (options.validate,options.command))
      get_zk_backupUri_sequence(options.host, options.port, filegenerated) 
     if("Restore" == options.command):
      log_info("***********options.validate:%s_%s*:" % (options.validate,options.command))
      validate_restore()
  
if __name__ == "__main__":
  main(sys.argv[1:])


