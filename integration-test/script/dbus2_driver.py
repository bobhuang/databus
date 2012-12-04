#!/usr/bin/env python

'''
  Start and stop dbus2 servers, consumers
  Will handle remote run in the future

  bootstrap_relay  start/stop
  bootstrap_producer  start/stop
  bootstrap_server  start/stop
  bootstrap_consumer  start/stop, stop_scn, stop_after_secs
  profile_relay
  profile_consumer
  
  zookeeper  start/stop/wait_exist/wait_no_exist/wait_value/cmd
$SCRIPT_DIR/dbus2_driver.py -c zookeeper -o start --zookeeper_server_ports=${zookeeper_server_ports}  --cmdline_props="tickTime=2000;initLimit=5;syncLimit=2" --zookeeper_cmds=<semicolon separate list of command> --zookeeper_path= zookeeper_value=
  -. start, parse the port, generate the local file path in var/work/zookeeper_data/1, start, port default from 2181, generate log4j file
  -. stop, find the process id, id is port - 2181 + 1, will stop all the processes
  -. wait, query client and get the status 
  -. execute the cmd

'''
__author__ = "David Zhang"
__version__ = "$Revision: 0.1 $"
__date__ = "$Date: 2010/11/16 $"
import sys, os, fcntl
import pdb
import time, copy, re
from optparse import OptionParser, OptionGroup
import logging
import threading
import pexpect
from utility import *
import distutils.dir_util
import json, urllib2
        
# Global varaibles
options=None
server_host="localhost"
server_port="9000"
consumer_host="localhost"
consumer_port=8081
consumer_http_start_port=8081     # may need to be changed?
consumer_jmx_service_start_port=10000     # may need to be changed?
rmi_registry_port="1099"
kill_cmd_template="jps | grep %s | cut -f1 -d\\  | xargs kill -9"
# kill_cmd_template="jps | grep %s | grep -v grep | grep -v testng | awk '{print $1}' | xargs kill -9 "
kill_container_template="ps -ef | grep tail | grep %s | awk '{print $2}' | xargs kill -9"
log_file_pattern="%s_%s_%s.%s.log"  # testname, component, oper, time, pid
stats_cmd_pattern='''jps | grep %%s | awk '{printf "open "$1"\\nbean com.linkedin.databus2:relayId=1408230481,type=OutboundTrafficTotalStats\\nget *"}' | java -jar %s/../lib/jmxterm-1.0-alpha-4-uber.jar -i -n''' % get_this_file_dirname()
#config_sub_cmd='''dbus2_config_sub.py''' % get_this_file_dirname()
jmx_cli = None

def zookeeper_opers(oper):
    zookeeper_setup(oper)
    globals()["zookeeper_opers_%s" % oper]()

def zookeeper_upload_schema():
	print("Uploading DB Schema for %s under %s" % (options.zookeeper_value, options.db_schema_dir))
	sys_call('integration-test/lib/espresso/dist-espresso-tools/bin/schema-uploader.sh --cluster-name %s --schema-dst-root %s --schema-src-root schemas_registry/%s --sync-method UPDATE --upload-db-alias %s --upload-db-regex %s --zk-address %s:%s' % (options.helix_clustername, os.environ["ZK_SCHEMA_ROOT_DIR"], options.db_schema_dir, options.zookeeper_value, options.zookeeper_value, options.zookeeper_server_hosts, options.zookeeper_server_ports))


def conf_and_deploy_GLU(target, action):
    
  # a workaround for glu-on-desktop "undeploy" failing occasionally
  if target == "null":
      return

  glu_cmd_template = "/usr/local/linkedin/bin/mint %s -w %s"
  cmd = glu_cmd_template % (("ng-%s"%action), target)
  print cmd
  sys_call(cmd)

  
def stop_gracefully(procname):
  kill_cmd="jps | grep %s | cut -f1 -d\\  | xargs kill" % (procname);
  sys_call(kill_cmd);
  timeout = 600;
  logFile = options.log_file
  wait_cmd = "jps | grep '%s' | wc -l" % (procname);
  ret = wait_for_condition('is_zero_output("%s")' % (wait_cmd), timeout,1.1)
  kill_cmd=kill_cmd_template % (procname)
  sys_call(kill_cmd);
  if ret == RetCode.TIMEOUT: print "Timed out waiting for process '%s' to die'" % (procname)
  exit(ret)
  

def get_stats(pattern):
    ''' called to get stats for a process '''
    pids = [x for x in sys_pipe_call_1("jps | grep %s" % pattern) if x]
    if not pids: my_error("pid for component '%s' ('%s') is not find" % (options.component, pattern))
    pid = pids[0].split()[0]
    get_stats_1(pid, options.jmx_bean, options.jmx_attr)

def wait_event(func, option=None):
    ''' called to wait for  '''
    wait_event_1(func(), option)

def producer_wait_event(name, func):
    ''' called to wait for  '''
    producer_wait_event_1(name, func())

def wait_for_log(func):
    timeout = func()
    print "Timeout is : %s" %timeout
    logFile = options.log_file
    msg = options.log_msg
    ret = wait_for_condition('found_in_log("%s", "%s")' % (logFile,msg), timeout,1.1)
    if ret == RetCode.TIMEOUT: print "Timed out waiting for message '%s' in log file '%s'" % (msg,logFile)
    exit(ret)


def stop_by_port():
    if not options.http_port:
        my_error("Error: http_port not specified" )
        return 
    sys_call("jps")
    pidnum_cmd = ("/sbin/fuser -n tcp %s 2>/dev/null | xargs kill -9" % (options.http_port))
    #pidnum_cmd = ( "fuser -n tcp %s" % (options.http_port) ) 
    ret = sys_call(pidnum_cmd)
    #print ret
    #kill_cmd2 = ("kill -9 %s" % (ret))
    #ret = os.popen(kill_cmd2).read()
    #print ret
    sys_call("jps")
    exit(ret)

    #    fuser_cmd_template = "fuser -k -n tcp %s 2>/dev/null"
    #    cmd1 = fuser_cmd_template % (options.http_port) 
    #    dbg_print('Stop by port cmd is : %s' % (cmd1))
    #    ret=sys_call(cmd1)
    #    exit(ret)

def rpl_dbus_stop_all_threads():
    print options.relay_port
    scriptFile = ("%s/rpl_dbus_stop_all_threads.bash" % (os.environ["SCRIPT_DIR"]))
    sys_call("/bin/bash %s %s" % (scriptFile, options.relay_port))
    return RetCode.OK

def rpl_dbus_start_all_threads():
    print options.relay_port
    scriptFile = ("%s/rpl_dbus_start_all_threads.bash" % (os.environ["SCRIPT_DIR"]))
    sys_call("/bin/bash %s %s" % (scriptFile, options.relay_port))
    return RetCode.OK

def rpl_dbus_delete_all_threads():
    scriptFile = ("%s/rpl_dbus_delete_threads.bash" % (os.environ["SCRIPT_DIR"]))
    sys_call("/bin/bash %s" % scriptFile)
    return RetCode.OK

def relay_wait_for_num_data_events():
    ''' options.relay_host should be set for remote_run '''
    relay_host = options.relay_host and options.relay_host or server_host
    relay_port = options.relay_port and options.relay_port or server_port
    if None == options.num_events_expected:
      print "--num_events_expected not specified"
      return RetCode.ERROR

    if options.sleep_before_wait: time.sleep(options.sleep_before_wait)
    url_template = "http://%s:%s/containerStats/inbound/events/total"
    numEvents = get_relay_inbound_total_numDataEvents(url_template, relay_host, relay_port, options.num_events_expected)
    dbg_print("numDataEvents = %s" % (numEvents))
    if numEvents == RetCode.TIMEOUT: print "Timed out waiting producer to reach numDataEvents %s" % options.num_events_expected
    return RetCode.OK

def relay_wait_for_version_refresh():
    relay_host = options.relay_host
    relay_port = options.relay_port
    db_name = options.db_name
    table_name = options.table_name
    expected_version = options.document_version
    actual_verion = -1
    trycount=0
    ret = RetCode.OK
    dbg_print ("Expecting Document Version = %s for %s.%s on relay %s:%s" % (str(expected_version), db_name, table_name, relay_host, str(relay_port)))
    while True:
        actual_version = get_max_version_in_relay_register(relay_host,relay_port,db_name,table_name)
        if int(expected_version) == int(actual_version):
            ret = RetCode.OK
            break
        else:
            if int(expected_version) < int(actual_version):
               print ("ERROR - Mismatch in Document Schema Version on relay %s:%s. Table: %s.%s. ExpectedVersion: %s, ActualVersion: %s" % (relay_host, str(relay_port), db_name, table_name,str(expected_version),str(actual_version)))
               ret = RetCode.ERROR
               break
            else:
                trycount = trycount + 1
                if trycount >= 10:
                   # wait a maximum of 1 minute
                   print ("ERROR - Waited 1 minute for relay schema refresh on relay %s:%s. Table: %s.%s. ExpectedVersion: %s, ActualVersion: %s" % (relay_host, str(relay_port), db_name, table_name,str(expected_version),str(actual_version)))
                   ret = RetCode.TIMEOUT
                   break
                time.sleep(6)

    exit(ret)
        

def shutdown(oper="normal"):
    pid = send_shutdown(server_host, options.http_port or server_port, oper == "force")
    dbg_print("shutdown pid = %s" % (pid))
    ret = wait_for_condition('not process_exist(%s)' % (pid), 120)

def get_wait_timeout():
    if options.timeout: return options.timeout
    else: return 10

def pause_resume_consumer(oper):
  # <sst> BUGBUG This needs to be fixed
    global consumer_port
    if options.component_id: consumer_port=find_open_port(consumer_host, consumer_http_start_port, options.component_id) 
    if options.http_port: consumer_port = options.http_port
    url = "http://%s:%s/pauseConsumer/%s" % (consumer_host, consumer_port, oper)
    out = send_url(url).split("\n")[1]
    dbg_print("out = %s" % out)
    time.sleep(0.1)

def get_inbound_total_maxStreamWinScn():
    port = options.http_port
    host = server_host
    print get_ebuf_inbound_total_maxStreamWinScn(host,port)
 
def get_inbound_total_minStreamWinScn():
    port = options.http_port
    host = server_host
    print get_ebuf_inbound_total_minStreamWinScn(host,port)
    
def get_bootstrap_db_conn_info():
    return ("bootstrap", "bootstrap", "bootstrap")

lock_tab_sql_file = tempfile.mkstemp()[1]
def producer_lock_tab(oper):
    dbname, user, passwd = get_bootstrap_db_conn_info()
    if oper == "lock" or oper == "save_file":
      qry = '''
drop table if exists lock_stat_tab_1;
CREATE TABLE lock_stat_tab_1 (session_id int) ENGINE=InnoDB;
drop procedure if exists my_session_wait;
delimiter $$
create procedure my_session_wait()
begin
  declare tmp int;
  LOOP
   select sleep(3600) into tmp;
  END LOOP;
end$$
delimiter ;

set @cid = connection_id();
insert into lock_stat_tab_1 values (@cid);
commit;
lock table tab_1 read local;
call my_session_wait(); 
unlock tables;
'''
      if oper == "save_file": open(lock_tab_sql_file, "w").write(qry)
      else:
        ret = mysql_exec_sql(qry, dbname, user, passwd)
        print ret
    #ret = cmd_call(cmd, options.timeout, "ERROR 2013", get_outf())
    else:
      ret = mysql_exec_sql_one_row("select session_id from lock_stat_tab_1", dbname, user, passwd)
      dbg_print(" ret = %s" % ret)
      if not ret: my_error("No lock yet")
      session_id = ret[0]
      qry = "kill %s" % session_id
      ret = mysql_exec_sql(qry, dbname, user, passwd)

def producer_purge_log():
    ''' this one is deprecated. Use the cleaner instead '''
    dbname, user, passwd = get_bootstrap_db_conn_info()
    ret = mysql_exec_sql("select id from bootstrap_sources", dbname, user, passwd, None, True)
    for srcid in [x[0] for x in ret]: # for each source
      dbg_print("srcid = %s" % srcid)
      applied_logid = mysql_exec_sql_one_row("select logid from bootstrap_applier_state", dbname, user, passwd)[0]
      qry = "select logid from bootstrap_loginfo where srcid=%s and logid<%s order by logid limit %s" % (srcid, applied_logid, options.producer_log_purge_limit)
      ret =  mysql_exec_sql(qry, dbname, user, passwd, None, True)
      logids_to_purge = [x[0] for x in ret]
      qry = ""
      for logid in logids_to_purge: qry += "drop table if exists log_%s_%s;" % (srcid, logid)
      mysql_exec_sql(qry, dbname, user, passwd)
      dbg_print("logids_to_purge = %s" % logids_to_purge)
      mysql_exec_sql("delete from bootstrap_loginfo where srcid=%s and logid in (%s); commit" % (srcid, ",".join(logids_to_purge)), dbname, user, passwd)
def espresso_cluster_reset_all():
     cmd_template = "integration-test/lib/espresso/espresso.sh reset_cluster_all"
     sys_call(cmd_template)
     print "espresso cluster reset all complete"

def espresso_cluster_reset():
     cmd_template = "integration-test/lib/espresso/espresso.sh reset_cluster %s %s"
     cmd1 = cmd_template % ("default" , "default")
     sys_call(cmd1)
     cmd2 = cmd_template % ("/export/apps/mysql-master2/my.cnf" , "default2")
     sys_call(cmd2)
     print "espresso cluster reset"

def cluster_start_single_node():
    if not options.espresso_node:
        my_error("Error: Espresso Node to start not defined" )
        return 
    cmd_template = "integration-test/lib/espresso/espresso.sh start_cluster_single_node %s"
    cmd = (cmd_template % options.espresso_node)
    sys_call(cmd)
    print "espresso node started"

def espresso_check_db_status(db_name, db_max_partitions):
    url_base_cmd = "curl -s \"http://localhost:12919/partitionstate?database=%s&state=master\""
    url_cmd= url_base_cmd % db_name 
    out = os.popen(url_cmd).readlines()
    out_list = []
    if out:
        if out[0].find('null') < 0:
            out_list=eval(out[0])
            return (len(out_list) == long(db_max_partitions))
        else:
            return False
    else:
        return False


def espresso_node_up():
    ret = 0
    timeout=300
    print ("Waiting for Espresso to load all schemas.....")
    if not options.db_list:
        my_error("Error: list of DB's not defined" )
        return 
    if not options.db_range:
        my_error ("Error: range of DB's not defined" )
        return

    config_file_base_list  = options.db_list.split(",")
    config_file_range_list = options.db_range.split(",") 

    for j,config_file_base in enumerate (config_file_base_list):
        ret = wait_for_condition('espresso_check_db_status("%s", %s)' % (config_file_base,config_file_range_list[j]), timeout)
        if ret == RetCode.TIMEOUT: 
            print "Timed out waiting for espresso DB %s to come up" % config_file_base
            return ret
        print ("db=%s up" % config_file_base)
    print ("done")
    return ret
def espresso_client_wait():
    ret = 0
    timeout=300
    print ("Waiting for Espresso clients to catchup.....")
    print options.db_list
    print options.db_range
    print options.client_base_port_list
    if not options.db_list:
        my_error("Error: list of DB's not defined" )
        return 
    if not options.db_range:
        my_error ("Error: range of DB's not defined" )
        return
    if not options.client_base_port_list:
        my_error ("Error: Client list of port bases not specified" )
        return

    db_list  = options.db_list.split(",")
    range_list = options.db_range.split(",") 
    port_base_list = options.client_base_port_list.split(",") 

    relay_host = options.relay_host and options.relay_host or server_host
    relay_port = options.relay_port and options.relay_port or server_port
    for j,subs in enumerate (db_list):
        for i in range(int(range_list[j])):
            consumer_port=int(port_base_list[j])+i
            url_template = "http://%s:%s/containerStats/inbound/events/psource/" + subs + "/" + str(i)
            maxWinScn = get_relay_inbound_total_maxStreamWinScn(url_template, relay_host, relay_port)
            ret = wait_for_condition('consumer_reach_maxStreamWinScn(%s, "%s", %s, "%s")' % (maxWinScn, consumer_host, consumer_port, ""), timeout)
            if ret == RetCode.TIMEOUT: 
                print ("Timed out waiting for espresso client %s:%s:%s to reach SCN:%s " % ( subs,i,port_base_list[j]+i, maxWinScn))
                return ret
    print ("client wait done!")
    return ret


def espresso_client_wait_53():
    ret = 0
    timeout=300
    interval = 5
    dbg_print ("Waiting for Espresso client to catchup.....")
    dbg_print ("DB List :" + options.db_list)
    dbg_print ("DB Range :" + str(options.db_range))
    dbg_print ("DB Partitions :" + str(options.db_partitions))
    dbg_print ("Client Ports :" + options.client_base_port_list)
    
    if not options.db_list:
        my_error("Error: list of DB's not defined" )
        return 
    if ( (not options.db_range) and (not options.db_partitions)):
        my_error (" Partition Number or range not specified !!")
        return
    if not options.client_base_port_list:
        my_error ("Error: Client list of port bases not specified" )
        return

    db_list  = options.db_list.split(",")
   
    range_list = []
    if options.db_range:
    	range_list = options.db_range.split(",") 
    elif options.db_partitions:
    	range_list = options.db_partitions.split(",") 
    
    client_port = options.client_base_port_list 

    for j,subs in enumerate (db_list):

        curr_Range = []
	if  options.db_range:
	    curr_range = range(int(range_list[j]))
        elif options.db_partitions :
	    curr_range = convert_range_to_list(range_list[j])

        for i in curr_range:
            consumer_port=client_port
            regIds=getRegistrationByPP(subs, i)
            for regId in regIds:
                dbg_print ("Registration Id :" + regId)
                relay_host_port = getCurrentRelay(regId)
                relay_host = None
                relay_port = None
                if (relay_host_port is None):
                    print("Unable to find Relay for the regId :" + regId )               
                    return RetCode.ERROR
                else:
                    (relay_host, sep, relay_port) = relay_host_port.partition("_")
                
                url_template = "http://%s:%s/containerStats/inbound/events/psource/" + subs + "/" + str(i)
                maxWinScn = get_relay_inbound_total_maxStreamWinScn(url_template, relay_host, relay_port)
                
                ret = wait_for_condition('consumer_reach_maxStreamWinScn(%s, "%s", %s, "%s")' % (maxWinScn, consumer_host, consumer_port, "registration/" + regId ), timeout, interval)
                        
                if ret == RetCode.TIMEOUT: 
                    print ("Timed out waiting for espresso client %s:%s:%s to reach SCN:%s for regId:%s" % ( subs,i,consumer_port, maxWinScn, regId))
                    return ret
                
    print ("client wait done!")
    return ret

def verifyRelayIdealState():
    storage_node_list = []
    storage_node_url = ("http://localhost:2100/clusters/%s/instances" % options.cluster_name )
    cmUrl = urllib2.urlopen(storage_node_url)
    cmOp  = cmUrl.read()
    dbg_print(cmOp)
    cmJson = json.loads(cmOp)
    for key, value in cmJson.iteritems():
        storage_node_list.append(key)

    print storage_node_list
 
def checkMasters():
    if not options.cluster_name:
        my_error ( "checkMasters: Cluster Name not specified" )
    if not options.db_list:
        my_error ( "checkMaster: DB Name not specified" )
    if not options.node_name:
        my_error ( "checkMaster: Node Name not specified" )
    retList = []
    Url = ("http://localhost:2100/clusters/%s/resourceGroups/%s/idealState" % (options.cluster_name, options.db_list) )
    dbg_print(Url)
    cmUrl = urllib2.urlopen(Url)
    cmOp  = cmUrl.read()
    dbg_print(cmOp)
    cmJson = json.loads(cmOp)
    var  = cmJson["listFields"]
    for key, value in var.iteritems():
        valStr = str(value)
        keyStr = str(key)
        if ( valStr .find(options.node_name) != -1 ):
            if ( keyStr .find("MASTER") != -1 ):
                retList. append(((str(keyStr.split(',')[2])).split('_')[0])[1:])
    print retList
       
def verifyRegistrations():
    if not options.expected_num_registrations:
        my_error ( "verifyRegistrations: expected_num_registrations not specified " )
    # Overload client_base_port_list here to specify a single port
    Url="http://localhost:" + options.client_base_port_list + "/clientStats/registrations"
    dbg_print(Url)
    clientUrlResp = urllib2.urlopen(Url).read()
    dbg_print(clientUrlResp)
    clientRespJson = json.loads(clientUrlResp).keys()
    dbg_print(clientRespJson)
    numRegs = len(clientRespJson)
    dbg_print(numRegs)
    if (numRegs == int(options.expected_num_registrations)):
         print("Number of registrations match expected number of registrations")
         ret=RetCode.OK
    else:
         print("Number of registrations (%d) DO NOT match expected number of registrations (%d)", numRegs, int(options.expected_num_registrations))
         ret=RetCode.ERROR
    exit(ret)
        
def verifyMpRegistrations():
    if not options.expected_num_registrations:
        my_error ( "verifyMpRegistrations: expected_num_registrations not specified " )
    numRegs = len(getMpRegistrations())
    dbg_print(numRegs)
    if (numRegs == int(options.expected_num_registrations)):
         print("Number of registrations match expected number of registrations")
         ret=RetCode.OK
    else:
         print("Number of registrations (%d) DO NOT match expected number of registrations (%d)", numRegs, int(options.expected_num_registrations))
         ret=RetCode.ERROR
    exit(ret)
 
def getRegistrations_list():
    regIds = []
    range_list = []
    if (not options.client_base_port_list):
        my_error ( " Client port not specified !!")
    if (not options.db_name):
        my_error ( " DBName not specified !!")
    if ( (not options.partition_num) and (not options.db_range) and (not options.db_partitions)):
        my_error (" Partition Number or range not specified !!")
    if ( options.partition_num and (options.db_range or options.db_partitions)):
        my_error ("Cannot specify both partition number and range")
    if ( options.partition_num ):
        regIds.append(getRegistrationByPP(options.db_name, options.partition_num))
    else:
        db_name = options.db_name.split(",")
        if options.db_range != None:
            range_list = options.db_range.split(",")
            for i,entry in enumerate(range_list):
                range_list[i] = ("0-%s" % (str(int(entry)-1)))
        elif options.db_partitions != False:
            range_list = options.db_partitions.split(",")
        for j,db_name in enumerate (db_name):
            for i in convert_range_to_list(range_list[j]):
                regIds.append(getRegistrationByPP(options.db_name, i))

    for regId in regIds:
        for rg in regId:
            print str(rg)

    return regIds
        
def getRegistrationByPP(db_name, partition_num):
    regId = get_registrations_by_physical_partition("localhost", options.client_base_port_list, db_name, partition_num, True, True)
    if (not regId):
        print "Registration not found for DB (%s) and partition_num (%s)"%(db_name,partition_num) 
        exit(RetCode.ERROR)
    return regId

def getMpRegistrations():
   mpRegIds = get_mp_registrations("localhost", options.client_base_port_list)
   if (not mpRegIds):
       print "Unable to find multipartition registration ids"
       exit(RetCode.ERROR)
   return mpRegIds

def PullerActive_bs_stub():
    PullerActive_list(True,False)
def PullerInactive_bs_stub():
    PullerActive_list(True, True)
def PullerInactive_relay_stub():
    PullerActive_list(False,True)

def obtainMaxSCN():
    retValue = RetCode.OK
    if ((not options.db_name) and ( not options.reg_id)):
        my_error ( " DBName or RegId not specified !!")
    if ( (not options.reg_id) and (not options.db_range) and (not options.db_partitions)):
        my_error (" One of (reg_id, db_range,db_partitions) must be  specified !!")
 
    if options.reg_id:
        regId=options.reg_id
        doObtainMaxScn("localhost", options.client_base_port_list, options.timeoutms, 1, regId, options.check_timeout)
        exit(RetCode.OK);
    elif options.db_name and (options.db_range or options.db_partitions):
        if options.db_range != False:
            range_list = options.db_range.split(",")
            for i,entry in enumerate(range_list):
                range_list[i] = ("0-%s" % (str(int(entry)-1)))
        if options.db_partitions is not None:
            range_list = options.db_partitions.split(",")

        db_list  = options.db_name.split(",")

        for j,db_name in enumerate (db_list):
            for i in convert_range_to_list(range_list[j]):
                db_name=str(db_name);
                regIds=getRegistrationByPP( db_name, i )
                for regId in regIds:
                    retValue = doObtainMaxScn("localhost", options.client_base_port_list, options.timeoutms, 1, regId, options.check_timeout)
                    if ( retValue != RetCode.OK ):
                        exit(retValue)
    exit(retValue)

def doObtainMaxScn(client_host, client_port, timeout,retry, registration_id, check_timeout=False):
    retValue = RetCode.OK
    Url = ("http://%s:%s/regops/fetchMaxScn/%s?maxScntimeoutPerRetry=%s&numMaxScnRetries=%s" % (client_host, str(client_port), str(registration_id),str(timeout),str(retry)))
    dbg_print(Url)
    clientUrlResp = urllib2.urlopen(Url).read()
    clientRespJson = json.loads(clientUrlResp)
    print (clientRespJson)
    success = clientRespJson.get('resultSummary',"FAIL");
    maxScn = clientRespJson.get('maxSCN', None);
    seq = None;

    if (maxScn is not None):
        seq = maxScn.get('sequence',"-1");
    relays = clientRespJson['maxScnRelays']; #resultSCNMap
    allrelays = clientRespJson['resultSCNMap'];
    if ( options.expected_servers != None ):
        if ( str(len(allrelays)) != options.expected_servers ):
            print "Num Relays: %s does not match expected servers %s" % (str(len(allrelays)), str(options.expected_servers))
            return RetCode.ERROR
    for j,relay in enumerate (relays):
        r = relay['address'];
        v = "%s,%s,%s,%s:%s" %(str(registration_id),str(success),str(seq),str(r.get('hostName','None')),str(r.get('port','-1')));
        print "%s" %(str(v))
    if ( check_timeout != True ):
        if ( success != "SUCCESS" ):
            retValue = RetCode.ERROR
    return retValue

def flush():
    retValue = RetCode.OK
    if ((not options.db_name) and (not options.reg_id)):
        my_error ( " DBName or RegId not specified !!")
    if ( (not options.reg_id) and (not options.db_range) and (not options.db_partitions)):
        my_error (" One of (reg_id, db_range,db_partitions) must be  specified !!")
 
    if options.reg_id:
        regId=options.reg_id
        retValue = doFlush("localhost", options.client_base_port_list, options.timeoutms, 1, options.timeoutms, regId, options.check_timeout)
        if (retValue != RetCode.OK):
            print("Flush call returned an Error")
            exit(retValue)
    elif options.db_name and (options.db_range or options.db_partitions):
        if options.db_range != False:
            range_list = options.db_range.split(",")
            for i,entry in enumerate(range_list):
                range_list[i] = ("0-%s" % (str(int(entry)-1)))
        if options.db_partitions is not None:
            range_list = options.db_partitions.split(",")

        db_list  = options.db_name.split(",")

        for j,db_name in enumerate (db_list):
            for i in convert_range_to_list(range_list[j]):
                db_name=str(db_name);
                regIds=getRegistrationByPP( db_name, i )
                for regId in regIds:
                   retValue = doFlush("localhost", options.client_base_port_list, options.timeoutms, 1, options.timeoutms, regId, options.check_timeout)
                   if (retValue != RetCode.OK):
                       print("Flush call returned an Error")
                       exit(retValue) 
    exit(retValue)


def doFlush(client_host, client_port, timeout,retry, flushTimeout, registration_id, check_timeout=False):
    Url = ("http://%s:%s/regops/flush/%s?maxScntimeoutPerRetry=%s&numMaxScnRetries=%s&flushTimeout=%s" % (client_host, str(client_port), str(registration_id),str(timeout),str(retry),str(flushTimeout)))
    print Url
    clientUrlResp = urllib2.urlopen(Url).read()
    clientRespJson = json.loads(clientUrlResp)
    print (clientRespJson)
    success = clientRespJson.get('resultStatus',"FAIL"); 
    currMaxScn = clientRespJson.get('currentMaxSCN', None);
    reqMaxScn = clientRespJson.get('requestedMaxSCN', None);
    
    currSeq = None;
    reqSeq = None;

    if (currMaxScn is not None):
       currSeq = currMaxScn.get('sequence',"-1");
    if (currMaxScn is not None):
       reqSeq = reqMaxScn.get('sequence',"-1");

    v = "%s,%s,%s,%s" %(str(registration_id),str(success),str(reqSeq),str(currSeq));
    print "%s" %(str(v))
    if ( check_timeout != True ):
        if  "MAXSCN_REACHED" in str(v):
            return RetCode.OK 
        else:
            return RetCode.ERROR 
    else:
        return RetCode.OK

def deregister(regId=None):
    retValue = RetCode.OK
    if ((not options.db_name) and ( not options.reg_id)):
        my_error ( " DBName or RegId not specified !!")
    if ( (not options.reg_id) and (not options.db_range) and (not options.db_partitions)):
        my_error (" One of (reg_id, db_range,db_partitions) must be  specified !!")
 
    if options.reg_id:
        regId=options.reg_id
        doDeregister("localhost", options.client_base_port_list, regId)
        exit(RetCode.OK);
    elif options.db_name and (options.db_range or options.db_partitions):
        if options.db_range != False:
            range_list = options.db_range.split(",")
            for i,entry in enumerate(range_list):
                range_list[i] = ("0-%s" % (str(int(entry)-1)))
        if options.db_partitions is not None:
            range_list = options.db_partitions.split(",")

        db_list  = options.db_name.split(",")

        for j,db_name in enumerate (db_list):
            for i in convert_range_to_list(range_list[j]):
                db_name=str(db_name);
                regIds=getRegistrationByPP( db_name, i )
                for regId in regIds:
                    doDeregister("localhost", options.client_base_port_list, regId)
    exit(retValue)
 

def doDeregister(client_host, client_port, registration_id):
    Url = ("http://%s:%s/regops/deregister/%s" % (client_host, str(client_port), str(registration_id)))
    dbg_print(Url)
    clientUrlResp = urllib2.urlopen(Url).read()
    clientRespJson = json.loads(clientUrlResp)
    dbg_print(clientRespJson)
    success = clientRespJson.get('success',"false"); 
    connShutdown = clientRespJson.get('connectionShutdown',"false");
    print "%s : %s,%s" %(str(registration_id),str(success),str(connShutdown))

def PullerActive_list(isBootstrapMode=False, isCheckInactive=False):
    retValue = RetCode.OK
    range_list=[]
    if options.reg_id:
        if not isBootstrapMode:
            if isCheckInactive:
                retValue = isRelayPullerActive(options.reg_id, True)
            else:
                retValue = isRelayPullerActive(options.reg_id, False)
        else:
            if isCheckInactive:
                retValue = isBootstrapPullerActive(options.reg_id, True)
            else:
                retValue = isBootstrapPullerActive(options.reg_id, False)
        exit(retValue)
    elif options.db_name and (options.db_range or options.db_partitions):
        db_name = options.db_name.split(",")
        if options.db_range != None:
            range_list = options.db_range.split(",")
            for i,entry in enumerate(range_list):
                range_list[i] = ("0-%s" % (str(int(entry)-1)))
        elif options.db_partitions != False:
            range_list = options.db_partitions.split(",")

        for j,db_name in enumerate (db_name):
            for i in convert_range_to_list(range_list[j]):
                regId=getRegistrationByPP( options.db_name, i )
                if not isBootstrapMode:
                    if isCheckInactive:
                        retValue=isRelayPullerActive(regId[0], True)
                    else:
                        retValue=isRelayPullerActive(regId[0], False)
                else:
                    if not isCheckInactive:
                        retValue=isBootstrapPullerActive(regId[0], True)
                    else:
                        retValue=isBootstrapPullerActive(regId[0], False)
                if retValue == RetCode.ERROR:
                    exit(retValue)
    exit(retValue)
                
def isRelayPullerActive(regId, checkForInactive=False):
    clientRespJson = getRegistrationStatusInfo(regId)
    status=clientRespJson["relayPullerComponentStatus"].lower();
    if ( (status == "initializing") or (status == "running" ) ):
        if checkForInactive:
            print "FALSE"
            return (RetCode.ERROR)
        else:
            print "TRUE"
            return (RetCode.OK)
    else:
        if checkForInactive:
            print "TRUE"
            return (RetCode.OK)
        else:
            print "FALSE"
            return (RetCode.ERROR)

def isBootstrapPullerActive(regId):
    clientRespJson = getRegistrationStatusInfo(regId)
    status=clientRespJson["bootstrapPullerComponentStatus"].lower();
    if ( (status == "initializing") or (status == "running" ) ):
        print "TRUE"
        return True
    else:
        print "FALSE"
        return False

def validateCandidateRelays(regId=""):
    if (regId == ""):
        regId = options.reg_id

    expected = list()
    if ( not options.expected_servers ):
	print "expected_servers not provided. Considering it as empty list !!"
    else:
    	expected = options.expected_servers.split(',')

    got = getCandidateRelays(regId);

    if (len(got) != len(expected)):
        print ("Expected and got Candidate Servers does not match in length. got : %s, expected : %s" % (str(got), str(expected)))
        exit (RetCode.ERROR)
      
    got.sort()
    expected.sort()
    
    if (got == expected):
       exit(RetCode.OK)
    else:
       print ("Expected and got Candidate Servers does not match. got : %s, expected : %s" % (str(got), str(expected)))
       exit (RetCode.ERROR)

'''
validateCallbacks operates at the level of multiPartition Consumer callback and matches if the number
of events received for the registration is the same as the number of callbacks dispatched for the events
'''
def validateCallbacks():
   if ( not options.client_base_port_list ):
       print "ERROR: client port is required input parameter"
       exit( RetCode.ERROR )
   if ( not options.reg_id ):
       print "ERROR: reg_id must be non-null"
       exit( RetCode.ERROR )

   Url1 = ("http://%s:%s/regops/stats/inbound/events/%s" % ("localhost", str(options.client_base_port_list), str(options.reg_id)))
   clientUrlResp1 = urllib2.urlopen(Url1).read()
   clientRespJson1 = json.loads(clientUrlResp1)
   numDataEventsReceived=int(clientRespJson1["numDataEvents"])

   Url2 = ("http://%s:%s/regops/stats/inbound/callbacks/%s" % ("localhost", str(options.client_base_port_list), str(options.reg_id)))
   clientUrlResp2 = urllib2.urlopen(Url2).read()
   clientRespJson2 = json.loads(clientUrlResp2)
   numDataEventsProcessed=int(clientRespJson2["numDataEventsProcessed"])
   numDataEventsProcessed=numDataEventsProcessed/2

   if ( options.num_events_expected ):
       numDataEventsExpected = int(options.num_events_expected)
   else:
       numDataEventsExpected = numDataEventsReceived
 
   print "validateCallbacks report: num of data events expected (%s) == num of data events processed (%s)" % ( str(numDataEventsExpected), str(numDataEventsProcessed))
   if (numDataEventsReceived == 0):
       print "validateCallbacks report: num of data events received are zero"
       exit (RetCode.ERROR)
   elif (numDataEventsExpected != numDataEventsProcessed):
       print "validateCallbacks report: num of data events expected (%s) != num of data events processed (%s)" % ( str(numDataEventsExpected), str(numDataEventsProcessed))
       exit (RetCode.ERROR)
   else:
       print "validateCallbacks report: num of data events expected (%s) == num of data events processed (%s)" % ( str(numDataEventsExpected), str(numDataEventsProcessed))
       exit (RetCode.OK)


def getCandidateRelays(regId=""):
    if (regId == ""):
        regId = options.reg_id

    clientRespJson = getRegistrationStatusInfo(regId)
    candidateRelays = clientRespJson["candidateRelays"]

    L = list();

    if ( not candidateRelays):
	print ""
        return L
    
    for relay in candidateRelays:
    	hostname = relay["address"]["address"]
    	port = relay["address"]["port"]
    	ret = "" + str(hostname) + "_" + str(port);
    	print ret
	L.append(ret)
    return L
	

def getCurrentConnections_bs_stub():
    getCurrentConnections_list(True)

def getCurrentConnections_list(isBootstrapMode=False):
    connectionList = []
    if options.reg_id:
        if not isBootstrapMode:
            connectionList.append(getCurrentRelay(options.reg_id))
        else:
            connectionList.append(getCurrentBootstrapServer(options.reg_id))
    elif options.db_name and (options.db_range or options.db_partitions):
        if options.db_range != False:
            range_list = options.db_range.split(",")
            for i,entry in enumerate(range_list):
                range_list[i] = ("0-%s" % (str(int(entry)-1)))
        if options.db_partitions != False:
            range_list = options.db_partitions.split(",")

        for j,db_name in enumerate (options.db_name):
            for i in convert_range_to_list(range_list[j]):
                regId=getRegistrationByPP( db_name, i )
                if not isBootstrapMode:
                    connectionList.append(getCurrentRelay(options.reg_id))
                else:
                    connectionList.append(getCurrentBootstrapServer(options.reg_id))
    return(connectionList)


def getCurrentRelay(regId=""):
    if (regId == ""):
        regId = options.reg_id

    clientRespJson = getRegistrationStatusInfo(regId)
    currRelay = clientRespJson["currentRelay"]
    if ( not currRelay):
        print "Relay for regId (" + regId + ") is null"
        return None
    hostname = currRelay["address"]["address"]
    port = currRelay["address"]["port"]
    ret = "" + str(hostname) + "_" + str(port);
    print ret
    return ret

def getCurrentBootstrapServer(regId=""):
    if (regId == ""):
        regId = options.reg_id

    clientRespJson = getRegistrationStatusInfo(regId)
    
    currBS = clientRespJson["currentBootstrapServer"]
    
    if ( not currBS):
        print "Bootstrap Server for regId (" + regId + ") is null"
        return None
    
    hostname = currBS["address"]["address"]
    port = currBS["address"]["port"]
    
    ret = "" + str(hostname) + "_" + str(port);
    print ret
    return ret
    
def getRegistrationStatusInfo(regId):
        # Overload client_base_port_list here to specify a single port
    clientRespJson = get_registration_status_info("localhost", options.client_base_port_list, regId)
    return clientRespJson

def extractEventFieldForRegStats(responseDict, fieldName):
    if (responseDict is None):
        return -1
    
    totalStats = responseDict["totalStats"]
    if (totalStats is not None):
        return totalStats[fieldName]
    
    return -1
    
def checkLeader():

    if not options.cluster_name:
        my_error ( "checkLeader: Cluster Name not specified" )
    if not options.node_name:
        my_error ( "checkLeader: Node Name not specified" )

    clustername = options.cluster_name
    nodename = options.node_name
    if options.check_ideal_state:
        Url  = ("http://localhost:2100/clusters/%s/resourceGroups/relayLeaderStandby/idealState" % options.cluster_name  )
    else:
        Url  = ("http://localhost:2100/clusters/%s/resourceGroups/relayLeaderStandby/externalView" % options.cluster_name  )
    dbg_print(Url)
    cmUrl  = urllib2.urlopen(Url)
    cmOp   = cmUrl.read()
    dbg_print(cmOp)
    cmJson = json.loads(cmOp)
    var    = cmJson["mapFields"]["relayLeaderStandby_0"][("%s" % options.node_name)]
    dbg_print(var)
    if (var == "LEADER"):
        print "SUCCESS"
        ret=RetCode.OK
    else:
        ret=RetCode.ERROR
        print "FAIL"
    exit(ret)

def start_oracle_db():
    print ("Not Implemented")

def stop_oracle_db():
    print ("Not Implemented")

def send_fake_relay_cmd():
    relay_host = options.relay_host and options.relay_host or server_host
    relay_port = options.relay_port and options.relay_port or server_port
    fake_cmd  = options.fake_cmd;
    url = "http://%s:%s/useFake?cmd=%s" % (relay_host, relay_port,fake_cmd);
    print("url = %s"% url)
    out = send_url(url).split("\n")
    print("out = %s" % out)

def send_unfake_relay_cmd():
    relay_host = options.relay_host and options.relay_host or server_host
    relay_port = options.relay_port and options.relay_port or server_port
    fake_cmd  = options.fake_cmd;
    url = "http://%s:%s/useReal?cmd=%s" % (relay_host, relay_port,fake_cmd);
    print("url = %s"% url)
    out = send_url(url).split("\n")
    print("out = %s" % out)

def start_espressorouter():
	log_file= os.path.join(get_work_dir(), (log_file_pattern % (options.component, options.operation, time.strftime('%y%m%d_%H%M%S'), os.getpid())))
        jvm_args="-Xms2g -Xmx2g -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseConcMarkSweepGC -XX:+HeapDumpOnOutOfMemoryError -Xloggc:${ESPRESSO_HOME}/gc.log"
        cmd1 = 'export JVM_ARGS=\"' + jvm_args + '\"'
        cmd2 = 'nohup integration-test/lib/espresso/dist-espresso-router/bin/run-espresso-router.sh %s zk %s %s:%s 12921 60000 > %s 2>&1 &' % (options.helix_clustername, os.environ["ZK_SCHEMA_ROOT_DIR"], options.zookeeper_server_hosts, options.zookeeper_server_ports, log_file)
	sys_call(cmd1 +' && '+ cmd2)

def stop_espressorouter():
	sys_call(kill_cmd_template % "EspressoRouterMain")

def movePartition_clustermanager():
	bounceStorageNodes = False #TODO Parameterize this in future
	
	#Error checking for nulls
	if not options.db_list:
		my_error ( "movePartition_clustermanager: db_list cannot be empty" )
	if not options.partition_num:
		my_error ( "movePartition_clustermanager: partition_num cannot be empty" )
	if not options.db_replicas:
		my_error ( "movePartition_clustermanager: db_replicas cannot be empty" )

	db_list  = options.db_list.split(",")
	replicas_list = options.db_replicas.split(",")
	partition_number = options.partition_num

	# stop storage node
	if bounceStorageNodes:
		sys_call('%s/dbus2_driver.py -n %s -c espresso_storage_node -o stop' % (os.environ["SCRIPT_DIR"], get_test_name()))
		time.sleep(5) # give time for storage node to properly stop

	for j,subs in enumerate (db_list):
		# Move partition - FileName has the following format: <DBName>_<Replicas>_<PartitionNoToMove>.json
		fileName = "%s/schemas_registry/%s/rebalancing/%s_%s_%s.json" % (os.environ["VIEW_ROOT"], options.db_schema_dir, subs, replicas_list[j], partition_number)

		# Upload the new ideal state
		sys_call('%s/dbus2_driver.py -n %s -c cluster_manager -o invoke --cluster_manager_props="--addIdealState %s %s %s --zkSvr %s:%s" ' % (os.environ["SCRIPT_DIR"], get_test_name(), options.helix_clustername, subs, fileName,  options.zookeeper_server_hosts, options.zookeeper_server_ports))

	if bounceStorageNodes:
		#Bounce Storage Nodes
		print("Bouncing Storage Nodes.");
		sys_call('%s/dbus2_driver.py -n %s -c espresso_storage_node -o cluster_start'%(os.environ["SCRIPT_DIR"], get_test_name()))
        print "******New Ideal State Uploaded. Waiting for Storage Node to compute New external View*******"
	time.sleep(45) # give time for storage node to start and move the partition
        if not options.no_increment_genid:
            sys_call('%s/dbus2_driver.py -n %s -c espresso_storage_node -o cluster_increment_genid'%(os.environ["SCRIPT_DIR"], get_test_name()))


def createCluster_clustermanager():
	# Add Cluster
	sys_call('%s/dbus2_driver.py -n %s -c cluster_manager -o invoke --cluster_manager_props="--addCluster %s --zkSvr %s:%s" ' % (os.environ["SCRIPT_DIR"], get_test_name(), options.helix_clustername, options.zookeeper_server_hosts, options.zookeeper_server_ports))

	# Add Instance(s)
	sys_call('integration-test/lib/espresso/helix-core-pkg/bin/helix-admin --addNode %s localhost:12818  -zkSvr %s:%s ' % (options.helix_clustername, options.zookeeper_server_hosts, options.zookeeper_server_ports))
	sys_call('integration-test/lib/espresso/helix-core-pkg/bin/helix-admin --addNode %s localhost:12918  -zkSvr %s:%s ' % (options.helix_clustername, options.zookeeper_server_hosts, options.zookeeper_server_ports))


def start_clustermanager():
	# Start Cluster Manager
	log_file= os.path.join(get_work_dir(), (log_file_pattern % (options.component, options.operation, time.strftime('%y%m%d_%H%M%S'), os.getpid())))
	sys_call('nohup integration-test/lib/espresso/helix-core-pkg/bin/run-helix-controller --cluster %s --mode STANDALONE --zkSvr %s:%s > %s 2>&1 &' % (options.helix_clustername, options.zookeeper_server_hosts, options.zookeeper_server_ports, log_file))

def stop_clustermanager():
	sys_call(kill_cmd_template % "HelixControllerMain")

def setup_clustermanager():
	#Error checking for nulls
	if not options.db_list:
		my_error ( "setup_clustermanager: db_list cannot be empty" )
	if not options.db_range:
		my_error ( "setup_clustermanager: db_range cannot be empty" )
	if not options.db_replicas:
		my_error ( "setup_clustermanager: db_replicas cannot be empty" )

	db_list  = options.db_list.split(",")
	range_list = options.db_range.split(",") 
	replicas_list = options.db_replicas.split(",")
	
	# Error Checking for correctness
	if len(db_list) != len(range_list):
		my_error ("db_list and range_list must have equal number of elements")
	if len(db_list) != len(replicas_list):
		my_error ("db_list and replicas_list must have equal number of elements")

        cmd2 = 'nohup integration-test/lib/espresso/espresso.sh setup'
	sys_call(cmd2)
	# start zookeeper
        if options.do_not_start_zookeeper == False:
	        zkhosts = ("%s:%s" % (options.zookeeper_server_hosts, options.zookeeper_server_ports))
	        sys_call('%s/dbus2_driver.py -n %s -c zookeeper -o start --zookeeper_server_ports=%s --zookeeper_reset' % (os.environ["SCRIPT_DIR"], get_test_name(), zkhosts))
	        if options.zookeeper_server_hosts != options.relay_zookeeper_server_hosts or options.zookeeper_server_ports != options.relay_zookeeper_server_ports:
		        # Start a separate zookeeper instance for relay cluster
		        zkhosts = ("%s:%s" % (options.relay_zookeeper_server_hosts, options.relay_zookeeper_server_ports))
		        sys_call('%s/dbus2_driver.py -n %s -c zookeeper -o start --zookeeper_server_ports=%s --zk_server_start_range=2 ' % (os.environ["SCRIPT_DIR"], get_test_name(), zkhosts))

	# Upload Schema
        if options.do_not_start_schemas_registry == False:
	        for j,subs in enumerate (db_list):
		        sys_call('%s/dbus2_driver.py -n %s -c zookeeper -o upload_schema --db_schema_dir=%s --zookeeper_value=%s --zookeeper_server_hosts=%s --zookeeper_server_ports=%s' % (os.environ["SCRIPT_DIR"], get_test_name(), options.db_schema_dir, subs, options.zookeeper_server_hosts, options.zookeeper_server_ports))
		        if options.zookeeper_server_hosts != options.relay_zookeeper_server_hosts or options.zookeeper_server_ports != options.relay_zookeeper_server_ports:
			        sys_call('%s/dbus2_driver.py -n %s -c zookeeper -o upload_schema --db_schema_dir=%s --zookeeper_value=%s --zookeeper_server_hosts=%s --zookeeper_server_ports=%s' % (os.environ["SCRIPT_DIR"], get_test_name(), options.db_schema_dir, subs, options.relay_zookeeper_server_hosts, options.relay_zookeeper_server_ports))

	# Wait for Schema Upload to refresh, start helix controller
	time.sleep(2)
	sys_call('%s/dbus2_driver.py -n %s -c cluster_manager -o createCluster --zookeeper_server_hosts=%s --zookeeper_server_ports=%s --helix_clustername=%s' % (os.environ["SCRIPT_DIR"], get_test_name(), options.zookeeper_server_hosts, options.zookeeper_server_ports, options.helix_clustername))

        #start cluster manager
        if options.do_not_start_helix_controller == False:
                sys_call('%s/dbus2_driver.py -n %s -c cluster_manager -o start --zookeeper_server_hosts=%s --zookeeper_server_ports=%s  --helix_clustername=%s' % (os.environ["SCRIPT_DIR"], get_test_name(), options.zookeeper_server_hosts, options.zookeeper_server_ports, options.helix_clustername))
	
	# Add DBs & Rebalance
	for j,subs in enumerate (db_list):
		sys_call('%s/dbus2_driver.py -n %s -c cluster_manager -o invoke --cluster_manager_props="--addResource %s %s %s MasterSlave --zkSvr %s:%s" ' % (os.environ["SCRIPT_DIR"], get_test_name(), options.helix_clustername, subs, range_list[j], options.zookeeper_server_hosts, options.zookeeper_server_ports))
		sys_call('%s/dbus2_driver.py -n %s -c cluster_manager -o invoke --cluster_manager_props="--rebalance %s %s %s --zkSvr %s:%s" ' % (os.environ["SCRIPT_DIR"], get_test_name(), options.helix_clustername, subs, replicas_list[j], options.zookeeper_server_hosts, options.zookeeper_server_ports))

	# Reset Storage Node(s)
	sys_call('%s/dbus2_driver.py -n %s -c espresso_storage_node -o cluster_reset_all' % (os.environ["SCRIPT_DIR"], get_test_name()))

	# Start Espresso Router
	sys_call('%s/dbus2_driver.py -n %s -c espresso_router -o start --zookeeper_server_hosts=%s --zookeeper_server_ports=%s' % (os.environ["SCRIPT_DIR"], get_test_name(), options.zookeeper_server_hosts, options.zookeeper_server_ports ))

	# Start Storage Node(s)
        if options.do_not_start_storage_nodes == False:
	    sys_call('%s/dbus2_driver.py -n %s -c espresso_storage_node -o cluster_start' % (os.environ["SCRIPT_DIR"], get_test_name()))


	time.sleep(15)
	print "Cluster Manager Setup & Started"

def teardown_clustermanager():
	#Stop Helix Controller
	sys_call('%s/dbus2_driver.py -n %s -c cluster_manager -o stop' % (os.environ["SCRIPT_DIR"], get_test_name()))
	
	#Stop Zookeeper
	sys_call('%s/dbus2_driver.py -n %s -c zookeeper -o stop --zookeeper_server_ports=%s:%s' % (os.environ["SCRIPT_DIR"], get_test_name(), options.zookeeper_server_hosts, options.zookeeper_server_ports))

	#Stop Espresso Router
	sys_call('%s/dbus2_driver.py -n %s -c espresso_router -o stop' % (os.environ["SCRIPT_DIR"], get_test_name()))

	# Stop Storage Node(s)
	sys_call('%s/dbus2_driver.py -n %s -c espresso_storage_node -o stop' % (os.environ["SCRIPT_DIR"], get_test_name()))

	print "Cluster Manager teardown complete"

def cluster_admin_client_start():
	sys_call(cm_admin_start_template % (options.zookeeper_server_hosts, options.zookeeper_server_ports))


cm_admin_start_template="integration-test/lib/espresso/helix-admin-webapp-pkg/bin/run-rest-admin --zkSvr %s:%s --port 2100 &"
cmd_dict_mp_beta={
    "test_relay":{"start":"databus-relay/databus-relay-run/run.py --Dtest=run","stop":kill_cmd_template % "HttpRelay","stats":[get_stats,"HttpRelay"]}
    ,"oracle_db":{"start":[start_oracle_db],"stop":[stop_oracle_db]}
    ,"test_bootstrap_producer":{"start":"databus-bootstrap-producer/databus-bootstrap-producer-run/run.py --Dtest=runproducer","stop":kill_cmd_template % "DatabusBootstrapProducer","lock_tab":"echo start; mysql -n -vvv -ubootstrap -pbootstrap -Dbootstrap -e 'source %s'" % lock_tab_sql_file,"unlock_tab":[producer_lock_tab,"unlock"], "producer_wait_event":[producer_wait_event,"producer",get_wait_timeout], "applier_wait_event":[producer_wait_event,"applier",get_wait_timeout],"clean_log":"databus-bootstrap-utils/databus-bootstrap-utils-run/run.py --Dtest=runcleaner","purge_log":[producer_purge_log]}  
    ,"bootstrap_server":{"start":"databus-bootstrap-server/databus-bootstrap-server-run/run.py --Dtest=run","stop":kill_cmd_template % "BootstrapHttpServer","shutdown":[shutdown]}
    ,"fault_bootstrap_server":{"start":"integration-test/integration-test-integ/run_fault.py --Dtest=run","stop":kill_cmd_template % "FaultInjectionBootstrapHttpServer","shutdown":[shutdown]}
    ,"bootstrap_seeder":{"start":"databus-bootstrap-utils/databus-bootstrap-utils-run/run.py --Dtest=runseeder","stop":kill_cmd_template % "BootstrapSeederMain"}
    ,"bootstrap_consumer":{"start":"databus-bootstrap-client/databus-bootstrap-client-run/run.py --Dtest=run-integrate","stop":kill_cmd_template % "IntegratedDummyDatabusConsumer"}
    ,"bootstrap_dbreset":{"default":"integration-test/script/bootstrap_dbreset.sh"}
    ,"profile_relay":{"start":"databus2-linkedin-relay/databus2-linkedin-relay-run/run.py --Dtest=run","stop": kill_cmd_template % "Member2RelayServer","stats":[get_stats,"Member2RelayServer"]}
    ,"bizfollow_relay":{"start":"databus2-linkedin-relay/databus2-linkedin-relay-run/run.py --Dtest=run-bizfollow","stop": kill_cmd_template % "BizFollowRelayServer","stats":[get_stats,"BizFollowRelayServer"],"shutdown":[shutdown]}
    ,"conn_relay":{"start":"databus2-linkedin-relay/databus2-linkedin-relay-run/run.py --Dtest=run-conn","stop": kill_cmd_template % "ConnectionsRelayServer","stats":[get_stats,"ConnectionsRelayServer"],"shutdown":[shutdown]}
    ,"liar_relay":{"start":"databus2-linkedin-relay/databus2-linkedin-relay-run/run.py --Dtest=run-liar","stop": kill_cmd_template % "LiarRelayServer","stats":[get_stats,"LiarRelayServer"],"maxscn":[get_inbound_total_maxStreamWinScn],"minscn":[get_inbound_total_minStreamWinScn]}
    ,"fault_relay":{"start":"databus2-linkedin-relay/databus2-linkedin-relay-run/run.py --Dtest=run-fault-relay","stop": kill_cmd_template % "FaultInjectionHttpRelay","stats":[get_stats,"FaultInjectionHttpRelay"],"maxscn":[get_inbound_total_maxStreamWinScn],"minscn":[get_inbound_total_minStreamWinScn], "fake":[send_fake_relay_cmd], "unfake":[send_unfake_relay_cmd], "shutdown":[shutdown]}
    ,"espresso_relay":{"start":"databus3-relay/databus3-relay-cmdline-pkg/script/run.py --Dtest=run","stop": [stop_gracefully,  "EspressoRelay"],"shutdown":[stop_by_port], "wait_for_version_refresh":[relay_wait_for_version_refresh], "wait_for_num_events":[relay_wait_for_num_data_events] }
    ,"espresso_client":{"start":"databus3-client/databus3-espresso-test-client/run.py --Dtest=run", "wait_event":[espresso_client_wait], "wait_event_53":[espresso_client_wait_53], "stop": kill_cmd_template % "EspressoTestDatabusClient", "verifyRegistrations" : [verifyRegistrations], "verifyMpRegistrations" : [verifyMpRegistrations],"getRegistrationsByPhysicalPartition" : [getRegistrations_list], "isRelayPullerActive" : [PullerActive_list], "deregister" : [deregister], "flush" : [flush], "fetchMaxScn" : [obtainMaxSCN], "isBootstrapPullerActive" : [PullerActive_bs_stub], "isRelayPullerInactive" :[PullerInactive_relay_stub], "isBootstrapPullerInactive" : [PullerInactive_bs_stub], "getCurrentRelay" : [getCurrentConnections_list], "getCurrentBootstrapServer" : [getCurrentConnections_bs_stub],  "getCandidateRelays" : [getCandidateRelays], "validateCandidateRelays" : [validateCandidateRelays], "validate_callbacks" : [validateCallbacks ] }
    ,"espresso_storage_node":{"start":"integration-test/lib/espresso/espresso.sh start","stop":"integration-test/lib/espresso/espresso.sh stop", "reset":"integration-test/lib/espresso/espresso.sh reset", "setup":"integration-test/lib/espresso/espresso.sh setup", "check_node_up" : [espresso_node_up], "cluster_reset" : [espresso_cluster_reset], "cluster_reset_all" : [espresso_cluster_reset_all], "cluster_start" : "integration-test/lib/espresso/espresso.sh start_cluster_config", "cluster_increment_genid" : "integration-test/lib/espresso/espresso.sh cluster_increment_genid", "stop_by_port": [stop_by_port], "cluster_start_single_node":[cluster_start_single_node]}
    ,"espresso_router":{ "start":[start_espressorouter], "stop":[stop_espressorouter] }
    #TODO FOR 5.3.3 Remove hard-coded ZK Server
    ,"cluster_admin_client":{"start": [cluster_admin_client_start], "checkLeader": [checkLeader] , "checkMasters":[checkMasters], "verifyRelayIdealState":[verifyRelayIdealState], "stop": kill_cmd_template % "RestAdminApplication"}
    #,"cluster_admin_client":{"start":"integration-test/lib/espresso/helix-admin-webapp-pkg/bin/run-rest-admin --zkSvr eat1-app207.stg:12913 --port 2100", "checkLeader": [checkLeader] , "checkMasters":[checkMasters], "verifyRelayIdealState":[verifyRelayIdealState], "stop": kill_cmd_template % "RestAdminApplication"}
    ,"cluster_manager":{"invoke":"integration-test/lib/espresso/cm.sh", "stop": [stop_clustermanager], "createCluster":[createCluster_clustermanager], "start":[start_clustermanager], "setup": [setup_clustermanager], "teardown": [teardown_clustermanager], "movePartition": [movePartition_clustermanager] }
    ,"init":{"settings":"echo \"settings initialized\""}
    ,"profile_consumer":{"start":"databus-profile-client/databus-profile-client-run/run.py --Dtest=run","stop": [stop_gracefully,"SimpleProfileConsumer"],"wait_event":[wait_event, get_wait_timeout],"wait_event_bootstrap":[wait_event, get_wait_timeout, "bootstrap"],"pause":[pause_resume_consumer,"pause"],"resume":[pause_resume_consumer, "resume"]}
     # container deploys are set to null, since ng deploys without containers ( The targets are still around for legacy support. Eventually they should be all cleaned up
    ,"db_relay":{"start":"databus2-linkedin-relay/databus2-linkedin-relay-run/run.py --Dtest=run-db-relay","stop":kill_cmd_template % "DBTestRelayServer","stats":[get_stats,"DatabusRelayMain"]}
    ,"relay_container":{"start":[conf_and_deploy_GLU, "null", "deploy"],"stop":[conf_and_deploy_GLU, "null", "undeploy"]}
    ,"relay_deploy":{"start":[conf_and_deploy_GLU, "******NO-IMPL*******", "deploy"],"stop":[conf_and_deploy_GLU, "*******NO-IMPL*******", "undeploy"]}
    ,"anet_relay_deploy":{"start":[conf_and_deploy_GLU, "databus2-relay-anet-war", "deploy"],"stop":[conf_and_deploy_GLU, "databus2-relay-anet-war","undeploy"]}
    ,"bizfollow_relay_deploy":{"start":[conf_and_deploy_GLU, "databus2-relay-bizfollow-war", "deploy"],"stop":[conf_and_deploy_GLU, "databus2-relay-bizfollow-war","undeploy"]}
    ,"cappr_relay_deploy":{"start":[conf_and_deploy_GLU, "databus2-relay-cappr-war", "deploy"],"stop":[conf_and_deploy_GLU, "databus2-relay-cappr-war","undeploy"]}
    ,"conn_relay_deploy":{"start":[conf_and_deploy_GLU, "databus2-relay-conn-war", "deploy"],"stop":[conf_and_deploy_GLU, "databus2-relay-conn-war","undeploy"]}
    ,"dev_relay_deploy":{"start":[conf_and_deploy_GLU, "databus2-relay-dev-war", "deploy"],"stop":[conf_and_deploy_GLU, "databus2-relay-dev-war","undeploy"]}
    ,"following_relay_deploy":{"start":[conf_and_deploy_GLU, "databus2-relay-following-war", "deploy"],"stop":[conf_and_deploy_GLU, "databus2-relay-following-war","undeploy"]}
    ,"forum_relay_deploy":{"start":[conf_and_deploy_GLU, "databus2-relay-forum-war", "deploy"],"stop":[conf_and_deploy_GLU, "databus2-relay-forum-war","undeploy"]}
    ,"liar_relay_deploy":{"start":[conf_and_deploy_GLU, "databus2-relay-liar-war", "deploy"],"stop":[conf_and_deploy_GLU, "databus2-relay-liar-war", "undeploy"]}
    ,"mbrrec_relay_deploy":{"start":[conf_and_deploy_GLU, "databus2-relay-mbrrec-war", "deploy"],"stop":[conf_and_deploy_GLU, "databus2-relay-mbrrec-war","undeploy"]}
    ,"member2_relay_deploy":{"start":[conf_and_deploy_GLU, "databus2-relay-member2-war", "deploy"],"stop":[conf_and_deploy_GLU, "databus2-relay-member2-war","undeploy"]}
    ,"news_relay_deploy":{"start":[conf_and_deploy_GLU, "databus2-relay-news-war", "deploy"],"stop":[conf_and_deploy_GLU, "databus2-relay-news-war","undeploy"]}
    ,"bootstrap_producer_container":{"start":[conf_and_deploy_GLU, "null", "deploy"],"stop":[conf_and_deploy_GLU, "null", "undeploy"]}
    ,"anet_bst_producer_deploy":{"start":[conf_and_deploy_GLU, "databus2-bootstrap-producer-anet-war", "deploy"],"stop":[conf_and_deploy_GLU, "databus2-bootstrap-producer-anet-war", "undeploy"]}
    ,"bizfollow_bst_producer_deploy":{"start":[conf_and_deploy_GLU, "databus2-bootstrap-producer-bizfollow-war", "deploy"],"stop":[conf_and_deploy_GLU, "databus2-bootstrap-producer-bizfollow-war", "undeploy"]}
    ,"cappr_bst_producer_deploy":{"start":[conf_and_deploy_GLU, "databus2-bootstrap-producer-cappr-war", "deploy"],"stop":[conf_and_deploy_GLU, "databus2-bootstrap-producer-cappr-war", "undeploy"]}
    ,"conn_bst_producer_deploy":{"start":[conf_and_deploy_GLU, "databus2-bootstrap-producer-conn-war", "deploy"],"stop":[conf_and_deploy_GLU, "databus2-bootstrap-producer-conn-war", "undeploy"]}
    ,"following_bst_producer_deploy":{"start":[conf_and_deploy_GLU, "databus2-bootstrap-producer-following-war", "deploy"],"stop":[conf_and_deploy_GLU, "databus2-bootstrap-producer-following-war", "undeploy"]}
    ,"forum_bst_producer_deploy":{"start":[conf_and_deploy_GLU, "databus2-bootstrap-producer-forum-war", "deploy"],"stop":[conf_and_deploy_GLU, "databus2-bootstrap-producer-forum-war", "undeploy"]}
    ,"liar_bst_producer_deploy":{"start":[conf_and_deploy_GLU, "databus2-bootstrap-producer-liar-war","deploy"],"stop":[conf_and_deploy_GLU, "databus2-bootstrap-producer-liar-war", "undeploy"]}
    ,"mbrrec_bst_producer_deploy":{"start":[conf_and_deploy_GLU, "databus2-bootstrap-producer-mbrrec-war", "deploy"],"stop":[conf_and_deploy_GLU, "databus2-bootstrap-producer-mbrrec-war", "undeploy"]}
    ,"member2_bst_producer_deploy":{"start":[conf_and_deploy_GLU, "databus2-bootstrap-producer-member2-war", "deploy"],"stop":[conf_and_deploy_GLU, "databus2-bootstrap-producer-member2-war", "undeploy"]}
    ,"news_bst_producer_deploy":{"start":[conf_and_deploy_GLU, "databus2-bootstrap-producer-news-war", "deploy"],"stop":[conf_and_deploy_GLU, "databus2-bootstrap-producer-news-war", "undeploy"]}
    ,"bootstrap_producer_deploy":{"start":[conf_and_deploy_GLU, "*******NO-IMPL********", "deploy"],"stop":[conf_and_deploy_GLU, "*****NO-IMPL*****", "undeploy"]}
    ,"bootstrap_server_container":{"start":[conf_and_deploy_GLU, "null", "deploy"],"stop":[conf_and_deploy_GLU, "null", "undeploy"]}
    ,"bootstrap_server_deploy":{"start":[conf_and_deploy_GLU,"databus2-bootstrap-server-war", "deploy" ],"stop":[conf_and_deploy_GLU, "databus2-bootstrap-server-war", "undeploy"]}
    ,"client_container":{"start":[conf_and_deploy_GLU, "null", "deploy"],"stop":[conf_and_deploy_GLU, "null", "undeploy"]}
    ,"test_consumer_deploy":{"start":[conf_and_deploy_GLU,"databus2-client-test-consumer-war", "deploy"],"stop":[conf_and_deploy_GLU, "databus2-test-client-war", "undeploy"]} 
    ,"test_multiclient_consumer_deploy":{"start":[conf_and_deploy_GLU,"databus2-multi-client-test-consumer-war", "deploy"],"stop":[conf_and_deploy_GLU, "databus2-multi-client-test-consumer-war", "undeploy"]} 
     ,"bizfollow_consumer":{"start":"databus-profile-client/databus-profile-client-run/run.py --Dtest=run-bizfollow","stop": [stop_gracefully,"SimpleBizfollowConsumer"],"wait_event":[wait_event, get_wait_timeout],"wait_event_bootstrap":[wait_event, get_wait_timeout, "bootstrap"],"pause":[pause_resume_consumer,"pause"],"resume":[pause_resume_consumer, "resume"],"shutdown":[shutdown],"shutdown_force":[shutdown,"force"]}
    ,"conn_consumer":{"start":"databus-profile-client/databus-profile-client-run/run.py --Dtest=run-conn","stop": [stop_gracefully,"SimpleConnectionsConsumer"],"wait_event":[wait_event, get_wait_timeout],"wait_event_bootstrap":[wait_event, get_wait_timeout, "bootstrap"],"pause":[pause_resume_consumer,"pause"],"resume":[pause_resume_consumer, "resume"],"shutdown":[shutdown],"shutdown_force":[shutdown,"force"]}
    ,"liar_consumer":{"start":"databus-profile-client/databus-profile-client-run/run.py --Dtest=run-liar","stop": [stop_gracefully, "SimpleLiarConsumer"],"wait_event":[wait_event, get_wait_timeout],"wait_event_bootstrap":[wait_event, get_wait_timeout, "bootstrap"],"maxscn":[get_inbound_total_maxStreamWinScn],"pause":[pause_resume_consumer,"pause"],"resume":[pause_resume_consumer, "resume"], "shutdown":[shutdown],"shutdown_force":[shutdown,"force"]}
    ,"zookeeper":{"start":[zookeeper_opers,"start"],"stop":[zookeeper_opers,"stop"],"wait_for_exist":[zookeeper_opers,"wait_for_exist"],"wait_for_nonexist":[zookeeper_opers,"wait_for_nonexist"],"wait_for_value":[zookeeper_opers,"wait_for_value"],"cmd":[zookeeper_opers,"cmd"], "upload_schema":[zookeeper_upload_schema]}
    ,"logs_check":{"wait_for_occurence":[wait_for_log,get_wait_timeout]}
    ,"rpl_dbus":{"delete_all_threads":[rpl_dbus_delete_all_threads], "stop_all_threads": [rpl_dbus_stop_all_threads], "start_all_threads":[rpl_dbus_start_all_threads]}
}

# use appropriate cmd dict according to version of build system
cmd_dict=cmd_dict_mp_beta

allowed_opers=[]
for cmd in cmd_dict: allowed_opers.extend(cmd_dict[cmd].keys())
allowed_opers=[x for x in list(set(allowed_opers)) if x!="default"]

cmd_ret_pattern={    # the pattern when the call is considered return successfully
    "test_relay_start":re.compile("Databus service started")
   ,"test_bootstrap_producer_start":re.compile("Databus service started")
   ,"test_bootstrap_producer_lock_tab":re.compile("lock table")
   ,"bootstrap_server_start":re.compile("Databus service started")
   ,"profile_relay_start":re.compile("Databus service started")
   ,"bizfollow_relay_start":re.compile("Databus service started")
   ,"liar_relay_start":re.compile("Databus service started")
   ,"fault_relay_start":re.compile("Databus service started")
   ,"bizfollow_relay_start":re.compile("Databus service started")
   ,"profile_consumer_start":re.compile("Sending /sources")
   ,"bizfollow_consumer_start":re.compile("Sending /sources|Waiting for leadership")
   ,"liar_consumer_start":re.compile("Sending /sources")
   ,"db_relay_start":re.compile("Databus service started")
   ,"relay_container_start":re.compile("\[com.linkedin.spring.log.cmdline.ServerCmdLineApp\] Running\.\.\.")
   ,"relay_deploy_start":re.compile("BUILD SUCCESSFUL")
   ,"liar_relay_deploy_start":re.compile("BUILD SUCCESSFUL")
   ,"bizfollow_relay_deploy_start":re.compile("BUILD SUCCESSFUL")
   ,"espresso_relay_start":re.compile("Databus service started")
   ,"espresso_client_start":re.compile("Databus service started")
   ,"liar_bst_producer_deploy_start":re.compile("BUILD SUCCESSFUL")
   ,"bizfollow_bst_producer_deploy_start":re.compile("BUILD SUCCESSFUL")
   ,"bootstrap_producer_container_start":re.compile("\[com.linkedin.spring.log.cmdline.ServerCmdLineApp\] Running\.\.\.")
   ,"bootstrap_producer_deploy_start":re.compile("BUILD SUCCESSFUL")
   ,"bootstrap_server_container_start":re.compile("\[com.linkedin.spring.log.cmdline.ServerCmdLineApp\] Running\.\.\.")
   ,"bootstrap_server_deploy_start":re.compile("BUILD SUCCESSFUL")
   ,"client_container_start":re.compile("\[com.linkedin.spring.log.cmdline.ServerCmdLineApp\] Running\.\.\.")
   ,"test_consumer_deploy_start":re.compile("BUILD SUCCESSFUL")
   ,"espresso_storage_node_start":re.compile("started espresso")
   ,"espresso_storage_node_reset":re.compile("reset espresso")
   ,"espresso_storage_node_stop":re.compile("stopped espresso")
   ,"espresso_storage_node_cluster_reset":re.compile("espresso cluster reset")
   ,"espresso_storage_node_cluster_increment_genid":re.compile("GenId Incremented")
   ,"espresso_storage_node_cluster_start_single_node":re.compile("espresso node started")
   ,"cluster_manager_invoke":re.compile("cluster admin done")
   ,"bootstrap_dbreset_default":re.compile("DBReinit Complete")
   ,"cluster_admin_client_start":re.compile("start")
#   ,"profile_consumer_start":re.compile("Starting http relay connection")
}

ct=None  # global variale of the cmd thread, use to access subprocess

# need to check pid to determine if process is dead
# Thread and objects
class cmd_thread(threading.Thread):
    ''' execute one cmd in parallel, check output. there should be a timer. '''
    def __init__ (self, cmd, ret_pattern=None, outf=None):
      threading.Thread.__init__(self)
      self.daemon=True      # make it daemon, does not matter if use sys.exit()
      self.cmd = cmd
      self.ret_pattern = ret_pattern
      self.outf = sys.stdout
      if outf: self.outf = outf
      self.thread_wait_end=False
      self.thread_ret_ok=False
      self.subp=None
      self.ok_to_run=True
    def run(self):
      self.subp = subprocess_call_1(self.cmd)
      print self.subp.pid
      if not self.subp: 
        self.thread_wait_end=True
        return
      # no block
      fd = self.subp.stdout.fileno()
      fl = fcntl.fcntl(fd, fcntl.F_GETFL)
      fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
      while (self.ok_to_run):  # for timeout case, must terminate the thread, need non block read 
        try: line = self.subp.stdout.readline()
        except IOError, e: 
          time.sleep(0.1)
          #dbg_print("IOError %s" % e)
          continue
        dbg_print("line = %s" % line)
        if not line: break
        self.outf.write("%s" % line)
        if not self.ret_pattern: break
        if self.ret_pattern and self.ret_pattern.search(line):
          self.thread_ret_ok=True
          break
      if not self.ret_pattern: self.thread_ret_ok=True   # no pattern ok
      self.thread_wait_end=True
      # has pattern but not find, then not ok
      #while (1):  # read the rest and close the pipe
      #  try: line = self.subp.stdout.readline()
      #  except IOError, e:
      #    break
      self.subp.stdout.close()
      dbg_print("end of thread run")

def cmd_call(cmd, timeout, ret_pattern=None, outf=None):
    ''' return False if timed out. timeout is in secs '''
    global ct
    ct = cmd_thread(cmd, ret_pattern, outf)
    ct.start()
    sleep_cnt = 0
    sleep_interval = 0.5
    ret = RetCode.TIMEOUT
    while (sleep_cnt * sleep_interval < timeout):
      #if not ct.is_alive:
      if ct.thread_wait_end or (ct.subp and not process_exist(ct.subp.pid)): 
        if ct.thread_ret_ok: ret = RetCode.OK  # include find pattern or no pattern given
        else: ret= RetCode.ERROR
        break    # done
      time.sleep(sleep_interval)
      sleep_cnt += 1
    while (not ct.thread_wait_end):
      ct.ok_to_run = False  # terminate the thread in timeout case
      time.sleep(0.1)
    return ret

remote_component=None
remote_cmd_template='''ssh %s "bash -c 'source /export/home/eng/dzhang/bin/jdk6_env; cd %s; %s'"'''
def run_cmd_remote_setup():
    print "!!! REMOTE RUN ENABLED !!!"
    global remote_component
    component_cnt = 0
    # find the one in the cfg file, so multiple consumers must be in sequence
    for section in remote_run_config:
      if re.search(options.component, section): 
        remote_component=section
        component_cnt +=1
        if not options.component_id or compnent_cnt == options.component_id: break
    if not remote_component: my_error("No section for component %s, id %s" % (options.component, options.component_id))
    remote_component_properties = remote_run_config[remote_component]
    set_remote_view_root(remote_component_properties["view_root"])
    # create the remote var/work dir, may not be needed as the current view have them
    #sys_call("ssh %s mkdir -p %s %s" % remote_run_config[remote_component]["host"], get_remote_work_dir(), get_remote_var_dir()
   
def run_cmd_remote(cmd):
    ret = remote_cmd_template % (remote_run_config[remote_component]["host"], get_remote_view_root(),  cmd)
    return ret

metabuilder_file=".metabuilder.properties"
def get_bldfwk_dir():
    bldfwk_file = "%s/%s" % (get_view_root(), metabuilder_file)
    bldfwk_dir = None
    if not os.path.exists(bldfwk_file): return None
    for line in open(bldfwk_file):
      m = re.search("(bldshared-[0-9]+)",line)
      if m: 
        bldfwk_dir= m.group(1)
        break
    print "Warning. Cannot find bldshared-dir, run bootstrap.xml"
    return bldfwk_dir

rsync_path="/usr/local/bin/rsync"
remote_deploy_cmd_template='''rsync -avz  --exclude=.svn --exclude=var --exclude=mmappedBuffer --exclude=eventLog --exclude=cp_com_linkedin_events --exclude=dist --rsync-path=%s %s/ %s:%s'''
remote_deploy_bldcmd_template='''rsync -avz  --exclude=.svn --rsync-path=%s %s %s:%s'''
remote_deploy_change_blddir_cmd_template='''ssh %s "sed 's/%s/%s/' %s > %s_tmp; mv -f %s_tmp %s" '''
def do_remote_deploy():
    global rsync_path
    if not remote_run:
      print "Remote config file is not set in var/config!"
      return RetCode.ERROR;
    bldfwk_dir = get_bldfwk_dir()
    view_root = get_view_root()
    for section in remote_run_config:
      remote_host = remote_run_config[section]["host"]
      remote_view_root = remote_run_config[section]["view_root"]
      if "rsync_path" in remote_run_config[section]: rsync_path=remote_run_config[section]["rsync_path"]
      remote_view_root_parent = os.path.dirname(remote_view_root)
      sys_call("ssh %s mkdir -p %s" % (remote_host, remote_view_root))
      cmd = remote_deploy_cmd_template % (rsync_path, view_root, remote_host, remote_view_root)
      sys_call(cmd) 
      if bldfwk_dir:
        cmd = remote_deploy_bldcmd_template % (rsync_path, os.path.join(os.path.dirname(view_root),bldfwk_dir), remote_host, remote_view_root_parent)
        sys_call(cmd) 
      # replace the metabuilder, TODO, escape the /
      metabuilder_full_path = os.path.join(remote_view_root, metabuilder_file)
      cmd = remote_deploy_change_blddir_cmd_template % (remote_host, view_root.replace("/","\/"), remote_view_root.replace("/","\/"), metabuilder_full_path,  metabuilder_full_path,  metabuilder_full_path,  metabuilder_full_path)
      sys_call(cmd) 
    return 0

def run_cmd_add_option(cmd, option_name, value, check_mp_alpha, check_exist=False):
    if check_exist:
      full_path = file_exists(value)
      if not full_path: my_error("File does not exists! %s" % value)
      value=full_path
    cmd_split=cmd.split()
    if isinstance(value, str) and value[0]!='"': 
      #value = value.replace(' ','\\ ')     # escape the white space
      value = '"%s"' % value   # quote it 
    if check_mp_alpha:
      cmd_split.insert(len(cmd_split)-1,"-D%s=%s" % (option_name, value))
    else:
      if "cm.props" in option_name:
          cmd_split.insert(len(cmd_split), "%s" % (value))
      else:
          cmd_split.insert(len(cmd_split)-1,"--D%s=%s" % (option_name, value))
    cmd = " ".join(cmd_split)
    dbg_print("cmd = %s" % cmd)
    return cmd

def run_cmd_add_log_file(cmd):
    log_file= os.path.join(get_work_dir(), (log_file_pattern % (options.component, options.operation, time.strftime('%y%m%d_%H%M%S'), os.getpid())))
    if options.logfile: log_file = options.logfile 
    #log_file = os.path.join(remote_run and get_remote_log_dir() or get_log_dir(), log_file)
    # TODO: maybe we want to put the logs in the remote host
    log_file = os.path.join(get_log_dir(), log_file)
    dbg_print("log_file = %s" % log_file)
    open(log_file,"w").write("TEST_NAME=%s\n" % get_test_name()) 
    # logging for all the command
    cmd += " 2>&1 | tee -a %s" % log_file 
    return cmd

def run_cmd_get_return_pattern():
    ret_pattern = None
    pattern_key = "%s_%s" % (options.component, options.operation)
    if pattern_key in cmd_ret_pattern: ret_pattern = cmd_ret_pattern[pattern_key]
    dbg_print("ret_pattern = %s" % ret_pattern)
    return ret_pattern

def run_cmd_setup():
    if re.search("_consumer",options.component): 
      global consumer_host
      if remote_run: consumer_host = remote_component_properties["host"]
      else: consumer_host = "localhost"
      dbg_print("consumer_host= %s" % consumer_host)

def run_cmd_add_config(cmd, check_mp_alpha=True):
    if options.operation in ["start","invoke", "clean_log"]: 
      if options.config: 
        if not remote_run: 
          cmd = run_cmd_add_option(cmd, "config.file", options.config, check_mp_alpha, check_exist=True)      # check exist will figure out
        else: 
          cmd = run_cmd_add_option(cmd, "config.file", os.path.join(get_remote_view_root(), options.config), check_mp_alpha, check_exist=False)  
      if options.dump_file: cmd = run_cmd_add_option(cmd, "dump.file", os.path.join(remote_run and get_remote_view_root() or get_view_root(), options.dump_file), check_mp_alpha)   
      if options.value_file: cmd = run_cmd_add_option(cmd, "value.dump.file", os.path.join(remote_run and get_remote_view_root() or get_view_root(), options.value_file), check_mp_alpha)   
      if options.log4j_file: cmd = run_cmd_add_option(cmd, "log4j.file", os.path.join(remote_run and get_remote_view_root() or get_view_root(), options.log4j_file), check_mp_alpha)   
      if options.jvm_direct_memory_size: cmd = run_cmd_add_option(cmd, "jvm.direct.memory.size", options.jvm_direct_memory_size, check_mp_alpha)   
      if options.jvm_max_heap_size: cmd = run_cmd_add_option(cmd, "jvm.max.heap.size", options.jvm_max_heap_size, check_mp_alpha)   
      if options.jvm_gc_log: cmd = run_cmd_add_option(cmd, "jvm.gc.log", options.jvm_gc_log, check_mp_alpha)   
      if options.jvm_args: cmd = run_cmd_add_option(cmd, "jvm.args", options.jvm_args, check_mp_alpha)   
      if options.db_config_file: 
          if options.db_config_file_range:
              config_file_list = ""
              config_file_base_list = options.db_config_file.split("," )
              config_file_range_list = options.db_config_file_range.split(",") 
              
              for j,config_file_base in enumerate (config_file_base_list):
                  for i in range(int(config_file_range_list[j])):
                      config_file_list += config_file_base + "_" + str(i) + ".json,"
              config_file_list = config_file_list.rstrip(',')
              options.db_config_file = config_file_list
          print options.db_config_file
          cmd = run_cmd_add_option(cmd, "db.relay.config", options.db_config_file, check_mp_alpha)   

      if options.cmdline_props: cmd = run_cmd_add_option(cmd, "cmdline.props", options.cmdline_props, check_mp_alpha)   
      if options.cm_props: cmd = run_cmd_add_option(cmd, "cm.props", options.cm_props, check_mp_alpha)   
      if options.filter_conf_file: cmd = run_cmd_add_option(cmd, "filter.conf.file", options.filter_conf_file, check_mp_alpha)   

      if options.checkpoint_dir: 
         if options.checkpoint_dir == "auto":
           checkpoint_dir = os.path.join(get_work_dir(), "databus2_checkpoint_%s_%s" % time.strftime('%y%m%d_%H%M%S'), os.getpid())
         else:
           checkpoint_dir = options.checkpoint_dir
         checkpoint_dir = os.path.join(remote_run and get_remote_view_root() or get_view_root(), checkpoint_dir) 
         cmd = run_cmd_add_option(cmd, "checkpoint.dir", checkpoint_dir,check_mp_alpha)   
         # clear up the directory
         if not options.checkpoint_keep and os.path.exists(checkpoint_dir): distutils.dir_util.remove_tree(checkpoint_dir)

      # options can be changed during remote run
      if remote_run: 
        remote_component_properties = remote_run_config[remote_component]
        if not options.relay_host and "relay_host" in remote_component_properties: options.relay_host = remote_component_properties["relay_host"]
        if not options.relay_port and "relay_port" in remote_component_properties: options.relay_port = remote_component_properties["relay_port"]
        if not options.bootstrap_host and "bootstrap_host" in remote_component_properties: options.bootstrap_host = remote_component_properties["bootstrap_host"]
        if not options.bootstrap_port and "bootstrap_port" in remote_component_properties: options.bootstrap_port = remote_component_properties["bootstrap_port"]
      if options.relay_host: cmd = run_cmd_add_option(cmd, "relay.host", options.relay_host, check_mp_alpha)   
      if options.relay_port: cmd = run_cmd_add_option(cmd, "relay.port", options.relay_port, check_mp_alpha)   
      if options.bootstrap_host: cmd = run_cmd_add_option(cmd, "bootstrap.host", options.bootstrap_host, check_mp_alpha)   
      if options.bootstrap_port: cmd = run_cmd_add_option(cmd, "bootstrap.port", options.bootstrap_port, check_mp_alpha)   
      if options.consumer_event_pattern: cmd = run_cmd_add_option(cmd, "consumer.event.pattern", options.consumer_event_pattern, check_mp_alpha)   
      if re.search("_consumer",options.component): 
        # next available port
        if options.http_port: http_port = options.http_port
        else: http_port = next_available_port(consumer_host, consumer_http_start_port)   
        cmd = run_cmd_add_option(cmd, "http.port", http_port, check_mp_alpha)
        cmd = run_cmd_add_option(cmd, "jmx.service.port", next_available_port(consumer_host, consumer_jmx_service_start_port), check_mp_alpha)   
    return cmd

def run_cmd():
    if (options.component=="bootstrap_dbreset"): setup_rmi("stop")
    if (not options.operation): options.operation="default"
    if (not options.testname): 
      options.testname = "TESTNAME" in os.environ and os.environ["TESTNAME"] or "default"
    if (options.operation not in cmd_dict[options.component]): 
      my_error("%s is not one of the command for %s. Valid values are %s " % (options.operation, options.component, cmd_dict[options.component].keys()))
    # handle the different connetion string for hudson
    if (options.component=="db_relay" and options.db_config_file): 
       options.db_config_file = db_config_change(options.db_config_file)
    if (options.component=="test_bootstrap_producer" and options.operation=="lock_tab"): 
      producer_lock_tab("save_file")
    cmd = cmd_dict[options.component][options.operation]  
    # cmd can be a funciton call
    if isinstance(cmd, list): 
      if not callable(cmd[0]): my_error("First element should be function"+cmd[0])
      ret = cmd[0](*tuple(cmd[1:]))        # call the function
      dbg_print("FuncRetCode=%s\n" % ret)
      if None == ret:
        return
      else:
        return ret
    if remote_run: run_cmd_remote_setup()
    check_mp_alpha = False 
    cmd = run_cmd_add_config(cmd, check_mp_alpha) # handle config file
    if remote_run: cmd = run_cmd_remote(cmd) 
    ret_pattern = run_cmd_get_return_pattern()
    cmd = run_cmd_add_log_file(cmd)
    ret = cmd_call(cmd, options.timeout, ret_pattern, get_outf())
    if options.operation == "stop": time.sleep(0.1)
    dbg_print("RetCode=%s\n" % ret)
    return ret

def setup_rmi_cond(oper):
    rmi_up = isOpen(server_host, rmi_registry_port)
    dbg_print("rmi_up = %s" % rmi_up)
    if oper=="start": return rmi_up
    if oper=="stop": return not rmi_up

def setup_rmi(oper="start"):
    ''' start rmi registry if not alreay started '''
    ret = RetCode.OK
    dbg_print("oper = %s" % oper)
    rmi_up = isOpen(server_host, rmi_registry_port)
    rmi_str = "python sitetools/rmiscripts/run.py; ./rmiservers/bin/rmiregistry%s" % oper
    if oper=="stop": sys_call(kill_cmd_template % "RegistryImpl")  # make sure it stops
    if (oper=="start" and not rmi_up) or (oper=="stop" and rmi_up):
      sys_call(rmi_str)
      # wait for rmi
      ret = wait_for_condition('setup_rmi_cond("%s")' % oper)

def setup_env():
    setup_rmi()

def get_outf():
    outf = sys.stdout
    if options.output: outf = open(options.output,"w")
    return outf

def start_jmx_cli():
    global jmx_cli
    if not jmx_cli:
      jmx_cli = pexpect.spawn("java -jar %s/../lib/jmxterm-1.0-alpha-4-uber.jar" % get_this_file_dirname())
      jmx_cli.expect("\$>")

def stop_jmx_cli():
    global jmx_cli
    if jmx_cli:
      jmx_cli.sendline("quit")
      jmx_cli.expect(pexpect.EOF)
      jmx_cli = None

def jmx_cli_cmd(cmd):
    if not jmx_cli: start_jmx_cli()
    dbg_print("jmx cmd = %s" % cmd)
    jmx_cli.sendline(cmd)
    jmx_cli.expect("\$>")
    ret = jmx_cli.before.split("\r\n")[1:]
    dbg_print("jmx cmd ret = %s" % ret)
    return ret

def get_stats_1(pid, jmx_bean, jmx_attr):
    outf = get_outf()
    start_jmx_cli()
    jmx_cli_cmd("open %s" % pid)
    ret = jmx_cli_cmd("beans")
    if jmx_bean=="list": 
      stat_re = re.compile("^com.linkedin.databus2:")
      stats = [x for x in ret if stat_re.search(x)]
      outf.write("%s\n" % "\n".join(stats))
      return
    stat_re = re.compile("^com.linkedin.databus2:.*%s$" % jmx_bean)
    stats = [x for x in ret if stat_re.search(x)]
    if not stats: # stats not find
      stat_re = re.compile("^com.linkedin.databus2:")
      stats = [x.split("=")[-1].rstrip() for x in ret if stat_re.search(x)]
      my_error("Possible beans are %s" % stats)
    full_jmx_bean = stats[0] 
    jmx_cli_cmd("bean %s" % full_jmx_bean)
    if jmx_attr == "all": jmx_attr = "*"
    ret = jmx_cli_cmd("get %s" % jmx_attr)
    outf.write("%s\n" % "\n".join(ret))
    stop_jmx_cli()

def run_testcase(testcase):
    dbg_print("testcase = %s" % testcase)
    os.chdir(get_testcase_dir()) 
    if not re.search("\.test$", testcase): testcase += ".test"
    if not os.path.exists(testcase): 
      os.chdir("espresso")
      if not os.path.exists(testcase): 
        my_error("Test case %s does not exist" % testcase)
    dbg_print("testcase = %s" % testcase)
    os.environ["TEST_NAME"]=testcase
    ret = sys_call("/bin/bash %s" % testcase)
    os.chdir(view_root)
    return ret

def get_relay_inbound_total_maxStreamWinScn(url_template, host, port):
    sleep_cnt = 0
    sleep_interval = 3
    timeout = 240
    #url_template = "http://%s:%s/containerStats/inbound/events/total"
    prevmaxScn = -1
    print "Calling maxStreamWinScn with url=" + url_template
    maxScn = http_get_field(url_template, host, port, "maxSeenWinScn")
    while (sleep_cnt * sleep_interval < timeout):
      dbg_print("attempt %s " % sleep_cnt)
      maxScn = http_get_field(url_template, host, port, "maxSeenWinScn")
      if long(maxScn) == long(prevmaxScn):
        dbg_print("success")
        ret = maxScn 
        break
      prevmaxScn = maxScn
      time.sleep(sleep_interval)
      sleep_cnt += 1
    return ret

def get_relay_inbound_total_numDataEvents(url_template, host, port, expEvents):
    sleep_cnt = 0
    timeout = get_wait_timeout()
    sleep_interval = timeout / 10
    ret=RetCode.TIMEOUT
    #url_template = "http://%s:%s/containerStats/inbound/events/total"
    print "Calling numDataEvents with url=" + url_template
    while (sleep_cnt * sleep_interval < timeout):
      dbg_print("attempt %s " % sleep_cnt)
      numEvents = http_get_field(url_template, host, port, "numDataEvents")
      if long(numEvents) >= long(expEvents):
        dbg_print("success")
        ret = numEvents
        break
      time.sleep(sleep_interval)
      sleep_cnt += 1
    return ret

def get_ebuf_inbound_total_maxStreamWinScn(host, port, option=None,  maxScnExtractor=None):
    url_template = "http://%s:%s/containerStats/inbound/events/total"
    if option == "bootstrap":
       url_template = "http://%s:%s/clientStats/bootstrap/events/total"
    elif ((option != "") and (option is not None)):
       url_template = "http://%s:%s/clientStats/inbound/events/" + option
    return http_get_field(url_template, host, port, "maxSeenWinScn", maxScnExtractor)

def get_ebuf_inbound_total_minStreamWinScn(host, port, option=None):
    url_template = "http://%s:%s/containerStats/inbound/events/total"
    if option == "bootstrap":
       url_template = "http://%s:%s/clientStats/bootstrap/events/total"
    elif ((option != "") and (option is not None)):
       url_template = "http://%s:%s/clientStats/inbound/events/" + option
    return http_get_field(url_template, host, port, "minScn")

def consumer_reach_maxStreamWinScn(maxWinScn, host, port, option=None, maxScnExtractor=None):
    consumerMaxWinScn = get_ebuf_inbound_total_maxStreamWinScn(host, port, option, maxScnExtractor)
    dbg_print("consumerMaxWinScn = %s, maxWinScn = %s" % (consumerMaxWinScn, maxWinScn))
    return long(consumerMaxWinScn) >= long(maxWinScn)

def found_in_log(log_file,msg):
    cmd = "grep -c '%s' %s" % (msg, log_file);
    out = sys_pipe_call(cmd);
    print out;
    return (long(out) > 0);

def is_zero_output(cmd):
    out = sys_pipe_call(cmd);
    print out;
    return (long(out) == 0);

def is_positive_output(cmd):
    out = sys_pipe_call(cmd);
    print out;
    return (long(out) > 0);



def producer_reach_maxStreamWinScn(name, maxWinScn):
    ''' select max of all the sources '''
    dbname, user, passwd = get_bootstrap_db_conn_info()
    tab_name = (name == "producer") and "bootstrap_producer_state" or "bootstrap_applier_state"
    qry = "select max(windowscn) from %s " % tab_name
    ret = mysql_exec_sql_one_row(qry, dbname, user, passwd)
    producerMaxWinScn = ret and ret[0] or 0   # 0 if no rows
    dbg_print("producerMaxWinScn = %s, maxWinScn = %s" % (producerMaxWinScn, maxWinScn))
    return long(producerMaxWinScn) >= long(maxWinScn)

def wait_for_condition(cond, timeout=60, sleep_interval = 0.1):
    ''' wait for a certain cond. cond could be a function. 
       This cannot be in utility. Because it needs to see the cond function '''
    dbg_print("cond = %s" % cond)
    sleep_cnt = 0
    ret = RetCode.TIMEOUT
    while (sleep_cnt * sleep_interval < timeout):
      dbg_print("attempt %s " % sleep_cnt)
      if eval(cond):
        dbg_print("success")
        ret = RetCode.OK
        break
      time.sleep(sleep_interval)
      sleep_cnt += 1
    return ret

def producer_wait_event_1(name, timeout):
    ''' options.relay_host should be set for remote_run '''
    relay_host = options.relay_host and options.relay_host or server_host
    relay_port = options.relay_port and options.relay_port or server_port
    if options.sleep_before_wait: time.sleep(options.sleep_before_wait)
    url_template = "http://%s:%s/containerStats/inbound/events/total"
    maxWinScn = get_relay_inbound_total_maxStreamWinScn(url_template, relay_host, relay_port)
    dbg_print("maxWinScn = %s, timeout = %s" % (maxWinScn, timeout))
    ret = wait_for_condition('producer_reach_maxStreamWinScn("%s", %s)' % (name,maxWinScn), timeout)
    if ret == RetCode.TIMEOUT: print "Timed out waiting producer to reach maxWinScn %s" % maxWinScn
    return ret

def send_shutdown(host, port, force=False):
    ''' use kill which is much faster '''
    #url_template = "http://%s:%s/operation/shutdown" 
    url_template = "http://%s:%s/operation/getpid" 
    pid = http_get_field(url_template, host, port, "pid")
    force_str = force and "-9" or ""
    sys_call("kill %s %s" % (force_str,pid))
    return pid

def wait_event_1(timeout, option=None):
    relay_host = options.relay_host and options.relay_host or server_host
    relay_port = options.relay_port and options.relay_port or server_port
    if options.p_src_id != None:
        url_template = "http://%%s:%%s/containerStats/inbound/events/psource/%s" % options.p_src_id
    else:
        url_template = "http://%s:%s/containerStats/inbound/events/total"
    maxWinScn = get_relay_inbound_total_maxStreamWinScn(url_template, relay_host, relay_port)
    print "Wait maxWinScn:%s" % maxWinScn
    dbg_print("maxWinScn = %s, timeout = %s" % (maxWinScn, timeout))
    # consumer host is defined already
    global consumer_port
    #remove <sst> BUGBUG
    if options.component_id: consumer_port=find_open_port(consumer_host, consumer_http_start_port, options.component_id) 
    if options.http_port: consumer_port = options.http_port

    ret = wait_for_condition('consumer_reach_maxStreamWinScn(%s, "%s", %s, "%s")' % (maxWinScn, consumer_host, consumer_port, option and option or ""), timeout)
    if ret == RetCode.TIMEOUT: print "Timed out waiting consumer to reach maxWinScn %s" % maxWinScn
    if options.sleep_after_wait: time.sleep(options.sleep_after_wait)
    return ret

zookeeper_cmd=None
zookeeper_server_ports=None
zookeeper_server_dir=None
zookeeper_server_sub_dir="zookeeper_data"
zookeeper_pid_file_name="zookeeper.pid"
zookeeper_server_ids=None
def zookeeper_setup(oper):
    global zookeeper_cmd, zookeeper_server_ports, zookeeper_server_dir, zookeeper_server_ids
    zookeeper_class= (oper=="start") and  "org.apache.zookeeper.server.quorum.QuorumPeerMain" or "org.apache.zookeeper.ZooKeeperMain"
    log4j_file=os.path.join(get_view_root(),"integration-test/config/zookeeper-log4j2file.properties")
    zookeeper_cp_file = os.path.join(get_view_root(), "build/integ-zookeeper/integ-zookeeper.classpath")
    with open(zookeeper_cp_file, "r") as cpfh:
      zookeeper_cp = cpfh.readline().strip()
    zookeeper_cmd="java -Xms1g -Xmx2g -Dlog4j.configuration=file://%s -cp %s %s" % (log4j_file, zookeeper_cp, zookeeper_class)
    print("zookeeper_cmd=%s" % (zookeeper_cmd))
    zookeeper_server_ports= options.zookeeper_server_ports 
    zookeeper_server_dir=os.path.join(get_work_dir(),zookeeper_server_sub_dir)
    zookeeper_server_ids= options.zookeeper_server_ids and [int(x) for x in options.zookeeper_server_ids.split(",")] or range(int(options.zk_server_start_range),len(zookeeper_server_ports.split(","))+int(options.zk_server_start_range))
    dbg_print("zookeeper_server_ids=%s" % (zookeeper_server_ids))

def zookeeper_opers_start_create_conf(zookeeper_server_ports_split):
    zookeeper_num_servers = len(zookeeper_server_ports_split)
    zookeeper_server_conf_files=[]
    zookeeper_internal_port_1_start = 2800
    zookeeper_internal_port_2_start = 3800
    # overide the default config
    # See zk documentation at http://zookeeper.apache.org/doc/r3.1.2/zookeeperStarted.html
    # initLimit:5 translates to a timeout of 10 seconds. syncLimit does not have any meaning without a quorum (>1 zookeepers). maxClientCnxns:0 translates to an unlimited connection pool
    server_conf={"tickTime":2000,"initLimit":5,"syncLimit":2,"maxClientCnxns":0}
    if options.cmdline_props:
      for pair in options.cmdline_props.split(";"):
        (k, v) = pair.split("=")
        if k in server_conf: server_conf[k] = v
    # get the server
    zookeeper_internal_conf=""
    for k in server_conf: zookeeper_internal_conf+="%s=%s\n" % (k, server_conf[k])
    dbg_print("zookeeper_internal_conf = %s" % zookeeper_internal_conf)
    for server_id in range(int(options.zk_server_start_range),zookeeper_num_servers+int(options.zk_server_start_range)):
      zookeeper_host = zookeeper_server_ports_split[server_id-int(options.zk_server_start_range)].split(":")[0]
      zookeeper_internal_port_1 = zookeeper_internal_port_1_start + server_id -1 
      zookeeper_internal_port_2 = zookeeper_internal_port_2_start +  server_id -1
      zookeeper_internal_conf += "server.%s=%s:%s:%s\n" % (server_id, zookeeper_host, zookeeper_internal_port_1, zookeeper_internal_port_2) 
    dbg_print("zookeeper_internal_conf = %s" % zookeeper_internal_conf)

    for server_id in range(int(options.zk_server_start_range),zookeeper_num_servers+int(options.zk_server_start_range)):
      if server_id not in zookeeper_server_ids: continue
      conf_file = os.path.join(zookeeper_server_dir,"conf_%s" % server_id)
      dataDir=os.path.join(zookeeper_server_dir,str(server_id))
      zookeeper_port = zookeeper_server_ports_split[server_id-int(options.zk_server_start_range)].split(":")[1]
      conf_file_p = open(conf_file, "w")
      conf_file_p.write("clientPort=%s\n" % zookeeper_port)
      conf_file_p.write("dataDir=%s\n" % dataDir)
      conf_file_p.write("%s\n" % zookeeper_internal_conf)
      conf_file_p.close()
      dbg_print("==conf file %s: \n %s" % (conf_file, open(conf_file).readlines()))
      zookeeper_server_conf_files.append(conf_file)
    return zookeeper_server_conf_files

def zookeeper_opers_start_create_dirs(zookeeper_server_ports_split):
    for server_id in range(int(options.zk_server_start_range),len(zookeeper_server_ports_split)+int(options.zk_server_start_range)):
      if server_id not in zookeeper_server_ids: continue
      current_server_dir=os.path.join(zookeeper_server_dir,str(server_id))
      dbg_print("current_server_dir = %s" % current_server_dir)
      if os.path.exists(current_server_dir): 
        if not options.zookeeper_reset: continue
        distutils.dir_util.remove_tree(current_server_dir)
      distutils.dir_util.mkpath(current_server_dir)
      my_id_file=os.path.join(current_server_dir, "myid")
      open(my_id_file,"w").write("%s\n" % server_id)
    
def zookeeper_opers_start():
    global ct, zookeeper_server_dir, zookeeper_pid_file_name
    zookeeper_pid_file = os.path.join(zookeeper_server_dir,zookeeper_pid_file_name)
    print("zookeeper pid file %s" % (zookeeper_pid_file))
    zookeeper_server_ports_split = zookeeper_server_ports.split(",")
    zookeeper_opers_start_create_dirs(zookeeper_server_ports_split)
    conf_files = zookeeper_opers_start_create_conf(zookeeper_server_ports_split)
    f = open(zookeeper_pid_file,"aw")
    for conf_file in conf_files:
      ret = cmd_call("%s %s" % (zookeeper_cmd, conf_file), 60, re.compile("My election bind port"))
      f.write("%s\n"%ct.subp.pid)
    f.close()

def zookeeper_opers_stop():
    global zookeeper_server_dir, zookeeper_pid_file_name
    zookeeper_pid_file = os.path.join(zookeeper_server_dir,zookeeper_pid_file_name)
    f = open ( zookeeper_pid_file, 'r')    
    pids = f.readlines()
    for pid in pids:
      kill_cmd = "kill -9 %s" % pid
      sys_call(kill_cmd)
    f.close()

def zookeeper_opers_cmd():
    if not options.zookeeper_cmds: 
      print "No zookeeper_cmds given"
      return
    splitted_cmds = ";".join(["echo %s" % x for x in options.zookeeper_cmds.split(";")])
    sys_call("(%s) | %s -server %s" % (splitted_cmds, zookeeper_cmd, zookeeper_server_ports))
 
def main(argv):
    # default 
    global options
    parser = OptionParser(usage="usage: %prog [options]")
    parser.add_option("-n", "--testname", action="store", dest="testname", default=None, help="A test name identifier")
    parser.add_option("", "--p_src_id", action="store", dest="p_src_id", default=None,
                       help="Physical Source ID.")
    parser.add_option("-c", "--component", action="store", dest="component", default=None, choices=cmd_dict.keys(),
                       help="%s" % cmd_dict.keys())
    parser.add_option("-o", "--operation", action="store", dest="operation", default=None, choices=allowed_opers,
                       help="%s" % allowed_opers)
    parser.add_option("", "--output", action="store", dest="output", default=None,
                       help="Output file name. Default to stdout")
    parser.add_option("", "--logfile", action="store", dest="logfile", default=None,
                       help="log file for both stdout and stderror. Default auto generated")
    parser.add_option("","--timeout", action="store", type="long", dest="timeout", default=600,
                       help="Time out in secs before waiting for the success pattern. [default: %default]")
    parser.add_option("","--timeoutms", action="store", type="long", dest="timeoutms", default=10000,
                       help="Time out in millisecs before waiting for the success pattern. [default: %default]")

    config_group = OptionGroup(parser, "Config options", "")
    config_group.add_option("-p", "--config", action="store", dest="config", default=None,
                       help="config file path")
    config_group.add_option("--dump_file", action="store", dest="dump_file", default=None,
                       help="Event dump file")
    config_group.add_option("--value_file", action="store", dest="value_file", default=None,
                       help="Event value dump file")
    config_group.add_option("-l", "--log4j_file", action="store", dest="log4j_file", default=None,
                       help="Log4j config file")
    config_group.add_option("", "--jvm_direct_memory_size", action="store", dest="jvm_direct_memory_size", default = None,
                       help="Set the jvm direct memory size. e.g., 2048m. Default using the one in ant file.")
    config_group.add_option("", "--jvm_max_heap_size", action="store", dest="jvm_max_heap_size", default = None,
                       help="Set the jvm head size. e.g., 1024m. Default using the one in ant file.")
    config_group.add_option("", "--jvm_args", action="store", dest="jvm_args", default = None,
                       help="Other jvm args. e.g., '-Xms24m -Xmx50m'")
    config_group.add_option("", "--jvm_gc_log", action="store", dest="jvm_gc_log", default = None,
                       help="Enable gc and give jvm gc log file")
    config_group.add_option("--log_file", action="store", dest="log_file", default=None,
                       help="Log File")
    config_group.add_option("--log_msg", action="store", dest="log_msg", default=None,
                       help="Log Message")
    config_group.add_option("--relay_host", action="store", dest="relay_host", default=None,
                       help="Host of relay for a consumer")
    config_group.add_option("--relay_port", action="store", dest="relay_port", default=None,
                       help="Port of relay for a consumer")
    config_group.add_option("--http_port", action="store", dest="http_port", default=None,
                       help="Http Port of the current started component")
    config_group.add_option("--db_config_file", action="store", dest="db_config_file", default=None,
                       help="DB relay config file")
    config_group.add_option("--fake_cmd", action="store", dest="fake_cmd", default=None,
                       help="Fake Command for Fault Injection")
    config_group.add_option("--db_config_file_range", action="store", dest="db_config_file_range", default=None,
                       help="DB relay config file range ( for different physical partitions)")
    config_group.add_option("--db_list", action="store", dest="db_list", default=None,
                       help="list of dbs in espresso")
    config_group.add_option("--is_mp_reg", action="store", dest="is_mp_reg", default=False,
                       help="is it an multipartition registration")
    config_group.add_option("--num_events_expected", action="store", dest="num_events_expected", default=None,
                       help="expected number of events in the registration")
    config_group.add_option("--db_name", action="store", dest="db_name", default=None,
                       help="db Name in espresso")
    config_group.add_option("--table_name", action="store", dest="table_name", default=None,
                       help="table Name in espresso")
    config_group.add_option("--document_version", action="store", dest="document_version", default=None,
                       help="Document version of table in espresso")
    config_group.add_option("--db_range", action="store", dest="db_range", default=None,
                       help="partition ranges for db_list ( for different physical partitions)")
    config_group.add_option("--db_partitions", action="store", dest="db_partitions", default=None,
                       help="discreet partition ranges for db_list")
    config_group.add_option("--partition_num", action="store", dest="partition_num", default=None,
                       help="partition number for db_list ( physical/logical partition)")
    config_group.add_option("--reg_id", action="store", dest="reg_id", default=None,
                       help="Registration id for the client")
    config_group.add_option("--db_replicas", action="store", dest="db_replicas", default=None,
                       help="Number of replicas to create for each database listed in db_list")
    config_group.add_option("--espresso_node", action="store", dest="espresso_node", default=None,
                       help="The number of the node to start")
    config_group.add_option("--db_schema_dir", action="store", dest="db_schema_dir", default="espresso_test",
		       help="Schema directory under schemas_registry where database schema definition files exist")
    config_group.add_option("--client_base_port_list", action="store", dest="client_base_port_list", default=None,
                       help="list of base port numbers for espresso clients")
    config_group.add_option("--cmdline_props", action="store", dest="cmdline_props", default=None,
                      help="command line props. Comma seperated parameters")
    config_group.add_option("--cluster_manager_props", action="store", dest="cm_props", default=None,
                       help="Command line cluster manager props. Comma separate cm parameter ")
    config_group.add_option("--cluster_name", action="store", dest="cluster_name", default=None,
                       help="name of cluster to connect to using cluster admin tool")
    config_group.add_option("--check_ideal_state", action="store_true", dest="check_ideal_state", default=False,
                       help="Use Ideal State instead of the external view for the Check Leader Function")
    config_group.add_option("--expected_num_registrations", action="store", dest="expected_num_registrations", default=None,
                       help="number of registrations expected in the client")
    config_group.add_option("--expected_servers", action="store", dest="expected_servers", default=None,
                       help="Candidate servers  expected in the client")
    config_group.add_option("--node_name", action="store", dest="node_name", default=None,
                       help="Name of relay node to get stats on using cluster admin tool ")
    config_group.add_option("--check_timeout", action="store_true", dest="check_timeout", default=False,
                       help="flag to give Flush API. The call will return true on Flush timesout if this flag is specified")
    config_group.add_option("--filter_conf_file", action="store", dest="filter_conf_file", default=None,
                       help="Filter conf file")
    config_group.add_option("--bootstrap_host", action="store", dest="bootstrap_host", default=None,
                       help="Host of bootstrap server")
    config_group.add_option("--bootstrap_port", action="store", dest="bootstrap_port", default=None,
                       help="Port of bootstrap server")
    config_group.add_option("--checkpoint_dir", action="store", dest="checkpoint_dir", default=None,
                       help="Client checkpoint dir")
    config_group.add_option("--checkpoint_keep", action="store_true", dest="checkpoint_keep", default=False,
                       help="Do NOT clean client checkpoint dir")
    config_group.add_option("--consumer_event_pattern", action="store", dest="consumer_event_pattern", default=None,
                       help="Check consumer event pattern if set")
    config_group.add_option("-x","--extservice_props", action="append", dest="extservice_props", default=None,
                       help="Config props to override the extservices. Can give multiple times. One for each property. <entry name>:<prop name>:value. e.g., databus2.relay.local.bizfollow: db.bizfollow.db_url ")
    test_case_group = OptionGroup(parser, "Testcase options", "")
    test_case_group.add_option("", "--testcase", action="store", dest="testcase", default = None,
                       help="Run a test. Report error. Default no test")

    stats_group = OptionGroup(parser, "Stats options", "")
    stats_group.add_option("","--jmx_bean", action="store", dest="jmx_bean", default="list",
                       help="jmx bean to get. [default: %default]")
    stats_group.add_option("","--jmx_att", action="store", dest="jmx_attr", default="all",
                       help="jmx attr to get. [default: %default]")

    remote_group = OptionGroup(parser, "Remote options", "")
    remote_group.add_option("", "--remote_run", action="store_true", dest="remote_run", default = False,
                       help="Run remotely based on config file. Default False")
    remote_group.add_option("", "--remote_deploy", action="store_true", dest="remote_deploy", default = False,
                       help="Deploy the source tree to the remote machine based on config file. Default False")
    remote_group.add_option("", "--remote_config_file", action="store", dest="remote_config_file", default = None,
                       help="Remote config file")

    zookeeper_group = OptionGroup(parser, "Zookeeper options", "")
    zookeeper_group.add_option("", "--zookeeper_server_hosts", action="store", dest="zookeeper_server_hosts", default = "localhost",
                       help="comma separated zookeeper hosts for storage node cluster, used to start/stop/connect to zookeeper")
    zookeeper_group.add_option("", "--zookeeper_server_ports", action="store", dest="zookeeper_server_ports", default = "2181",
                       help="comma separated zookeeper ports for storage node cluster, used to start/stop/connect to zookeeper")
    zookeeper_group.add_option("", "--relay_zookeeper_server_hosts", action="store", dest="relay_zookeeper_server_hosts", default = "localhost",
                       help="comma separated zookeeper hosts for relay cluster, used to start/stop/connect to zookeeper")
    zookeeper_group.add_option("", "--relay_zookeeper_server_ports", action="store", dest="relay_zookeeper_server_ports", default = "2181",
                       help="comma separated zookeeper ports for relay cluster, used to start/stop/connect to zookeeper")
    zookeeper_group.add_option("", "--zookeeper_path", action="store", dest="zookeeper_path", default = None,
                       help="the zookeeper path to wait for")
    zookeeper_group.add_option("", "--zookeeper_value", action="store", dest="zookeeper_value", default = None,
                       help="zookeeper path value")
    zookeeper_group.add_option("", "--zookeeper_cmds", action="store", dest="zookeeper_cmds", default = None,
                       help="cmds to send to zookeeper client. Comma separated ")
    zookeeper_group.add_option("", "--zookeeper_server_ids", action="store", dest="zookeeper_server_ids", default = None,
                       help="Comma separated list of server to start. If not given, start the number of servers in zookeeper_server_ports. This is used to start server on multiple machines ")
    zookeeper_group.add_option("", "--zookeeper_reset", action="store_true", dest="zookeeper_reset", default = False,
                       help="If true recreate server dir, otherwise start from existing server dir")
    zookeeper_group.add_option("", "--zk_server_start_range", action="store", dest="zk_server_start_range", default = "1",
                       help="Range to start ZK server ID from")

    helix_group = OptionGroup(parser, "Helix options", "")
    helix_group.add_option("","--helix_clustername", action="store", dest="helix_clustername", default="DevCluster_Dbus",
			help="cluster name to control using the helix controller")
    helix_group.add_option("","--no_increment_genid", action="store_true", dest="no_increment_genid", default=False,
			help="Do not increment generation id while swapping partitions")
    helix_group.add_option("","--do_not_start_helix_controller", action="store_true", dest="do_not_start_helix_controller", default=False,
			help="Do not start helix controller for storage node")
    helix_group.add_option("","--do_not_start_zookeeper", action="store_true", dest="do_not_start_zookeeper", default=False,
			help="Do not start zookeeper for storage node")
    helix_group.add_option("","--do_not_start_schemas_registry", action="store_true", dest="do_not_start_schemas_registry", default=False,
			help="Do not start schemas registry for storage node")
    helix_group.add_option("","--do_not_start_storage_nodes", action="store_true", dest="do_not_start_storage_nodes", default=False,
			help="Do not start storage node Instances")

    other_option_group = OptionGroup(parser, "Other options", "")
    other_option_group.add_option("", "--component_id", action="store", dest="component_id", default = None,
                       help="The compnent id (1,2..) if there are mutliple instance of a component")
    parser.add_option("","--sleep_before_wait", action="store", type="long", dest="sleep_before_wait", default=0,
                       help="Sleep secs before waiting consumer reaching maxEventWindowScn. [default: %default]")
    parser.add_option("","--sleep_after_wait", action="store", type="long", dest="sleep_after_wait", default=1,
                       help="Sleep secs after consumer reaching maxEventWindowScn. [default: %default]")
    parser.add_option("","--producer_log_purge_limit", action="store", type="int", dest="producer_log_purge_limit", default=1000,
                       help="The limit on number of logs to purge for producer [default: %default]")
    debug_group = OptionGroup(parser, "Debug options", "")
    debug_group.add_option("-d", "--debug", action="store_true", dest="debug", default = False,
                       help="debug mode")
    debug_group.add_option("--ant_debug", action="store_true", dest="ant_debug", default = False,
                       help="ant debug mode")
    debug_group.add_option("", "--sys_call_debug", action="store_true", dest="enable_sys_call_debug", default = False,
                       help="debug sys call")
    parser.add_option_group(config_group)
    parser.add_option_group(test_case_group)
    parser.add_option_group(stats_group)
    parser.add_option_group(remote_group)
    parser.add_option_group(zookeeper_group)
    parser.add_option_group(other_option_group)
    parser.add_option_group(helix_group)
    parser.add_option_group(debug_group)


    (options, args) = parser.parse_args()
    set_debug(options.debug)
    set_sys_call_debug(options.enable_sys_call_debug)
    dbg_print("options = %s  args = %s" % (options, args))

    arg_error=False
    if not options.component and not options.testcase and not options.remote_deploy:
       print("\n!!!Please give component!!!\n")
       arg_error=True
    if arg_error: 
      parser.print_help()
      parser.exit()

    setup_env()
    if options.testcase:
      ret = run_testcase(options.testcase)
      if ret!=0: ret=1     # workaround a issue that ret of 256 will become 0 after sys.exit
      sys.exit(ret)
    if (not options.testname):
      options.testname = "TEST_NAME" in os.environ and os.environ["TEST_NAME"] or "default"
    os.environ["TEST_NAME"]= options.testname;
    
    if (not "WORK_SUB_DIR" in os.environ): 
        os.environ["WORK_SUB_DIR"] = "log"
    if (not "LOG_SUB_DIR" in os.environ):
        os.environ["LOG_SUB_DIR"] = "log"
    setup_work_dir()

    if options.remote_deploy or options.remote_run:
      if options.remote_config_file:
        remote_config_file = file_exists(options.remote_config_file)
        if not remote_config_file: my_error("remote_config_file %s does not existi!!" % options.remote_config_file)
        dbg_print("remote_config_file = %s" % remote_config_file)
        global remote_run, remote_config
        remote_config = parse_config(remote_config_file)
        remote_run=True
      if options.remote_deploy: 
        sys_call_debug_begin()
        ret = do_remote_deploy()
        sys_call_debug_end()
        sys.exit(ret)
    sys_call_debug_begin()
    ret = run_cmd()
    sys_call_debug_end()
    sys.exit(ret)
    
if __name__ == "__main__":
    main(sys.argv[1:])


