'''
====  utilities
'''

import sys, os, subprocess
import socket, pdb, re
import urllib, errno
import time, shutil
import tempfile
import json, urllib2
import traceback

sys_call_debug=False
enable_sys_call_debug=False
debug_enabled=False
host_name_global = (os.popen("/bin/hostname").read()).split(".")[0]

view_root=None
test_name=None
this_file_full_path=os.path.abspath(__file__)
this_file_dirname=os.path.dirname(this_file_full_path)
work_dir=None
log_dir=None
var_dir=None
var_dir_template="%s/integration-test/var"
testcase_dir=None
testcase_dir_template="%s/integration-test/testcases"
cwd_dir=os.getcwd()
# used to run cmd, can combine multiple command
components=[
     "test_relay"
    ,"test_bootstrap_producer"
    ,"bootstrap_server"
    ,"bootstrap_consumer"
    ,"profile_relay"
    ,"profile_consumer"
]

def dbg_print(in_str):
    #import pdb
    #pdb.set_trace()
    if debug_enabled:
        print ("== " + sys._getframe(1).f_code.co_name + " == " + str(in_str))

def sys_pipe_call(cmd):
    dbg_print("%s:%s" % (os.getcwd(),cmd))
    if sys_call_debug:
        print("cmd = %s " % cmd)
        if re.search("svn (log|info)",cmd): return os.popen(cmd).read()
        return ""
    return os.popen(cmd).read()               

def get_this_file_dirname(): return this_file_dirname
#handle the json import 
if sys.version_info[0]==2 and sys.version_info[1]<6:
  try:
    import simplejson as json
  except: 
    out=sys_pipe_call(os.path.join(get_this_file_dirname(),"install_python_packages.sh"))
    #print("install json = %s " % out)
    import simplejson as json
else:
  import json

# functions

def get_rpldbus_threadname_from_hostname(mysqlCoordinate):
    host_name = ""
    port_num = ""
    host_and_port = mysqlCoordinate.split(':')
    if len(host_and_port) > 0:
       host_name = host_and_port[0]
    if len(host_and_port) > 1:
       port_num = host_and_port[1]

    # rpl dbus manager replaces '.' with '_' when creating threads in rpl dbus, and uses the fqdn of host. Example "localhost:14100" will resolve to threadname such as "mgandhi-ld_linkedin_biz:14100"
    thread_name = ("%s:%s" % (host_name.replace('.','_'), str(port_num)))

    dbg_print ("thread name is %s" % thread_name)
    return thread_name

def getfqdn(host_name):
    return sys_pipe_call("hostname --fqdn").strip()

def get_rpldbus_nodes(relay_host, relay_port):
	dbg_print ("inside get_rpldbus_nodes=%s:%s?pretty" % (relay_host, relay_port))
    	Url = ("http://%s:%s/rplDbusAdmin/rplDbusStates" % (relay_host, str(relay_port)))
    	dbg_print(Url)
    	relayUrlResp = urllib2.urlopen(Url).read()
    	relayRespJson = json.loads(relayUrlResp)
    	dbg_print(relayRespJson)
	rpldbus_nodes = {} #map of all the nodes by thread name
	# result is and array of stateOjbects. 
	# we'll extract slave/thread name and use it as a key
	for i in range(0, len(relayRespJson)):
		node = relayRespJson[i]
		thread_name = node['liRplSlaveName']				
		dbg = "%s=>%s" % (thread_name, node)
		dbg_print(dbg)
		rpldbus_nodes[thread_name] = node
    	return rpldbus_nodes

#get list of the current event buffers in the relay
def get_relay_buffers(relay_host, relay_port):
	dbg_print ("inside get_relay_buffers=%s:%s" % (relay_host,relay_port))
    	Url = ("http://%s:%s/physicalBuffers" % (relay_host, str(relay_port)))
    	dbg_print(Url)
    	relayUrlResp = urllib2.urlopen(Url).read()
    	relayRespJson = json.loads(relayUrlResp)
    	dbg_print(relayRespJson)
	parts = [] #list of partitions avaialbe on this relay
        if (len(relayRespJson) == 0):
            return parts
	for k in relayRespJson.keys():
		pSource = relayRespJson[k]
		#each key is a json oject itself
		key = json.loads(k) 
		#print "keys is" +  key
		for v in pSource:
			key_str = "%s_%s_%s" % (key['name'],key['id'],v['role']['role'])
			print "%s => %s" % (key_str, v['uri'])
			parts.append(key_str)
    	return parts

def get_registrations(client_host, client_port):
    Url = ("http://%s:%s/clientState/registrations" % (client_host, str(client_port)))
    dbg_print(Url)
    clientUrlResp = urllib2.urlopen(Url).read()
    clientRespJson = json.loads(clientUrlResp)
    dbg_print(clientRespJson)
    return clientRespJson

def get_client_cluster_registrations(client_host, client_port, cluster_name):
    Url = ("http://%s:%s/clientState/clientPartitions/%s" % (client_host, str(client_port), cluster_name))
    dbg_print("Url is : %s"%Url)
    clientUrlResp = urllib2.urlopen(Url).read()
    clientRespJson = json.loads(clientUrlResp)
    dbg_print(clientRespJson)
    return clientRespJson


def get_mp_registrations(client_host, client_port):
    Url = ("http://%s:%s/clientState/mpRegistrations" % (client_host, str(client_port)))
    dbg_print(Url)
    clientUrlResp = urllib2.urlopen(Url).read()
    clientRespJson = json.loads(clientUrlResp)
    dbg_print(clientRespJson)
    return clientRespJson

def lookup_mprid_rid(rid, client_host, client_port):
    mp_regs = get_mp_registrations(client_host, client_port)
    for mpRegId in mp_regs:
        values = mp_regs[mpRegId]
        if regId in values:
            return mpRegId;
    print "Could not find an mp regId for the given regId"
    return RetCode.ERROR

def get_registration_status_info(client_host, client_port, registration_id):
    Url = ("http://%s:%s/clientState/registration/%s" % (client_host, str(client_port), str(registration_id)))
    dbg_print(Url)
    clientUrlResp = urllib2.urlopen(Url).read()
    clientRespJson = json.loads(clientUrlResp)
    dbg_print(clientRespJson)
    return clientRespJson

def get_relay(client_host, client_port, registration_id):
    mpRegId = lookup_mprid_rid(registration_id, client_host, client_port)
    if (mpRegId != None and mpRegId != RetCode.ERROR):
        registration_id = mpRegId
    clientRespJson = get_registration_status_info(client_host, client_port, registration_id)
    currRelay = clientRespJson["currentRelay"]
    
    if ( not currRelay):
        print "Relay for regId (" + regId + ") is null"
        return None
    
    hostname = currRelay["address"]["address"]
    port = currRelay["address"]["port"]
    
    relayName = ("%s:%s" % (str(hostname), str(port)))
    return relayName

def get_relays(client_host, client_port, db_name, partition_num, master, slave):
    regIds = get_registrations_by_physical_partition(client_host, client_port, db_name, partition_num, master, slave)
    relay_list = list()
    for regId in regIds:
        relay = get_relay(client_host, client_port, regId)
        if relay != None:
            relay_list.append(relay)
    return relay_list

def get_child_registrations(client_host, client_port, cluster_name):
  regIds = list() 
  clientRespJson = get_client_cluster_registrations(client_host,client_port,cluster_name);
  for i in range(0, len(clientRespJson)):
    node = clientRespJson[i]
    regIds.append(node["regId"]["id"])
  return regIds

def get_registrations_by_physical_partition(client_host, client_port, db_name, partition_num, master, slave):
    if master == False and slave == False:
        my_error("Master and Slave flags cannot be set to False. Atleast one flag must be set to True")
    dbg_print("Looking for db %s, partition %s, client: %s:%s, Master = %s, Slave = %s" %(db_name, partition_num, client_host, client_port, str(master), str(slave)))
    regIds = list()
    clientRespJson = get_registrations(client_host, client_port)
    for key in clientRespJson.keys():
        dbg_print("---- key %s ----" % key)
        dbg_print("Comparing name %s and %s" % (clientRespJson[key][0]["physicalPartition"]["name"].lower(), db_name.lower()))
        dbg_print("Comparing id %s and %s" % (int(clientRespJson[key][0]["physicalPartition"]["id"]), int(partition_num)))
        if clientRespJson[key][0]["physicalPartition"]["name"].lower() ==  db_name.lower() and int(clientRespJson[key][0]["physicalPartition"]["id"]) == int(partition_num):
            if master and clientRespJson[key][0]["physicalSource"]["role"]["role"].lower() == "master":
                regIds.append(key)
            elif slave and clientRespJson[key][0]["physicalSource"]["role"]["role"].lower() == "slave":
                regIds.append(key)
            elif clientRespJson[key][0]["physicalSource"]["role"]["role"].lower() == "any":
                regIds.append(key)
    return regIds

def get_relay_sources(relay_host, relay_port):
    Url = ("http://%s:%s/sources" % (relay_host, str(relay_port)))
    dbg_print(Url)
    relayUrlResp = urllib2.urlopen(Url).read()
    relayRespJson = json.loads(relayUrlResp)
    dbg_print(relayRespJson)
    return relayRespJson

def get_relay_source(relay_host, relay_port, db_name, table_name):
    relayRespJson = get_relay_sources(relay_host, relay_port)
    fullTableName = ("%s.%s" % (db_name, table_name))
    for dbsource in relayRespJson:
        if dbsource["name"] == fullTableName:
           return dbsource["id"]
    return None

def get_relay_registers(relay_host, relay_port, db_name, table_name):
    relaySourceId = get_relay_source(relay_host, relay_port, db_name, table_name)
    Url = ("http://%s:%s/register?sources=%s" % (relay_host, str(relay_port), str(relaySourceId)))
    relayUrlResp = urllib2.urlopen(Url).read()
    print relayUrlResp
    relayRespJson = json.loads(relayUrlResp)
    dbg_print(relayRespJson)
    return relayRespJson

def get_max_version_in_relay_register(relay_host, relay_port, db_name, table_name):
    try:
       relayRespJson = get_relay_registers(relay_host, relay_port, db_name, table_name)
    except:
       print ("Unexpected Error trying to find schema version of table %s, db %s, relay %s:%s:" % (table_name, db_name, relay_host, str(relay_port)))
       traceback.print_stack()
       return -1 #-1 is returned if get_relay_registers returns error

    versionList = list()
    for source in relayRespJson:
        versionList.append(source["version"])
    maxversion = max(versionList)
    print("Max Version of %s.%s on relay %s:%s is %s" % (db_name, table_name, relay_host, str(relay_port), str(maxversion)))
    return maxversion


def setup_view_root():
    global view_root, testcase_dir
    if "VIEW_ROOT" in os.environ: view_root = os.environ["VIEW_ROOT"]
    else: view_root= os.path.abspath("%s/../../" % this_file_dirname)
    #print("view_root = %s" % view_root)
    os.chdir(view_root)
    os.environ["VIEW_ROOT"]=view_root
    testcase_dir= testcase_dir_template % (view_root)


def get_view_root(): return view_root

def setup_work_dir():
    global var_dir, work_dir, log_dir, test_name
    var_dir= var_dir_template % (view_root)
    import distutils.dir_util
    distutils.dir_util.mkpath(var_dir, verbose=1)

    if "TEST_NAME" in os.environ: 
        test_name=os.environ["TEST_NAME"]
        #else: assert False, "TEST NAME Not Defined"
    else: test_name="UnknownTest"
    if "WORK_SUB_DIR" in os.environ: 
        work_dir=os.path.join(var_dir,os.environ["WORK_SUB_DIR"],test_name)
        #else: assert False, "Work Dir Not Defined"
    else:
        work_dir=os.path.join(var_dir,"log",test_name)
    if "LOG_SUB_DIR" in os.environ: 
        log_dir=os.path.join(var_dir, os.environ["LOG_SUB_DIR"], test_name)
    else: 
        #assert False, "Work Dir Not Defined"
        log_dir=os.path.join(var_dir, "log", test_name)
    distutils.dir_util.mkpath(work_dir, verbose=1)  
    distutils.dir_util.mkpath(log_dir, verbose=1)

def get_test_name(): return test_name
def get_work_dir(): return work_dir
def get_log_dir(): return log_dir
def get_var_dir(): return var_dir
def get_script_dir(): return get_this_file_dirname()
def get_testcase_dir(): return testcase_dir
def get_cwd(): return cwd_dir

def file_exists(file):  # test both 
    ''' return the abs path of the file if exists '''
    if os.path.isabs(file): 
      if os.path.exists(file): return file
      else: return None
    tmp_file=os.path.join(view_root, file)
    if os.path.exists(tmp_file): return tmp_file
    tmp_file=os.path.join(cwd_dir,file)
    if os.path.exists(tmp_file): return tmp_file
    return None
  
def set_debug(flag): 
    global debug_enabled
    debug_enabled=flag

def set_sys_call_debug(flag): 
    global enable_sys_call_debug
    enable_sys_call_debug=flag

def sys_call_debug_begin():
    if not enable_sys_call_debug: return
    global sys_call_debug
    sys_call_debug=True
    
def sys_call_debug_end():
    if not enable_sys_call_debug: return
    global sys_call_debug
    sys_call_debug=False

def sys_call(cmd):
    dbg_print("%s:%s" % (os.getcwd(),cmd))
    if sys_call_debug:
        print("cmd = %s " % cmd)
        return
    return os.system(cmd)

def subprocess_call_1(cmd, outfp=None):
    dbg_print("%s:%s" % (os.getcwd(), cmd))
    if not sys_call_debug:
        if outfp:
          p = subprocess.Popen(cmd, shell=True, stdout=outfp, stderr=outfp, close_fds=True)
        else:
          p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
        dbg_print("subprocess pid = %s" % p.pid)
        return p
    else:
        print("cmd = %s " % cmd)
        return None

def sys_pipe_call_4(cmd):
    dbg_print("%s:%s" % (os.getcwd(), cmd))
    if not sys_call_debug:
        p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, close_fds=True)
        dbg_print("subprocess pid = %s" % p.pid)
        return p.stdout
    else:
        None

def sys_pipe_call_3(cmd):
    dbg_print("%s:%s" % (os.getcwd(), cmd))
    if not sys_call_debug:
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, close_fds=True)
        dbg_print("subprocess pid = %s" % p.pid)
        #p = os.popen(cmd)
        return (p.stdout, p.pid)
    else:
        None

def sys_pipe_call_5(cmd):
    ''' return both stdin, stdout and pid '''
    dbg_print("%s:%s" % (os.getcwd(), cmd))
    if not sys_call_debug:
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
        dbg_print("subprocess pid = %s" % p.pid)
        #p = os.popen(cmd)
        return (p.stdout, p.stderr, p.pid)
    else:
        None

def sys_pipe_call_21(input, cmd):
    ''' call with input pipe to the cmd '''
    dbg_print("%s:%s:%s" % (os.getcwd(),input, cmd))
    if not sys_call_debug:
      return  subprocess.Popen(cmd.split(), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True).communicate(input)[0]
    else:
      return  ""

def sys_pipe_call_2(input, cmd):
    ''' call with input pipe to the cmd '''
    dbg_print("%s:%s:%s" % (os.getcwd(),input, cmd))
    if not sys_call_debug:
      return  subprocess.Popen(cmd.split(), stdin=subprocess.PIPE, stdout=subprocess.PIPE, close_fds=True).communicate(input)[0]
    else:
      return  ""

def sys_pipe_call_1(cmd):
    ''' also return the errors '''
    dbg_print("%s:%s" % (os.getcwd(),cmd))
    if not sys_call_debug:
      p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
      return p.stdout.readlines()
    else:
      return  ""

#    dbg_print("%s:%s" % (os.getcwd(),cmd))
#    return os.popen4(cmd)[1].read()

def sys_call_env(cmd):
    cmds=cmd.split()
    dbg_print("cmds= %s " % cmds)
    os.spawnv( os.P_WAIT, cmds[0], cmds[1:])

def whoami():
    return sys._getframe(1).f_code.co_name

def my_error(s):
    if debug_enabled: assert False, "Error: %s" % s
    else: 
      print "ERROR: %s" % s
      sys.exit(1)

def my_warning(s):
    if debug_enabled:
        print ("== " + sys._getframe(1).f_code.co_name + " == " + str(s))
    else: 
      print "WARNING: %s" % s
    #logger.warning(s)

def enter_func():
    dbg_print ("Entering == " + sys._getframe(1).f_code.co_name + " == ")

def get_time():
    return float("%0.4f" % time.time())   # keep 2 digits

def isOpen(ip,port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
      s.connect((ip, int(port)))
      s.shutdown(2)
      return True
    except:
      return False

def next_available_port(ip,port):
    port_num = int(port)
    while (isOpen(ip, port_num)): port_num +=1
    return port_num

def find_open_port(host, start_port, seq_num):
    ''' find the seq_num th port starting from start_port ''' 
    limit = 100
    start_port_num = int(start_port)
    seq = 0
    for i in range(limit):
      port_num = start_port_num + i
      if isOpen(host, port_num): seq += 1
      if seq == seq_num: return port_num
    return None

def process_exist(pid):
    try:
        os.kill(int(pid), 0)
    except OSError, err:
        if err.errno == errno.ESRCH: return False # not running
        elif err.errno == errno.EPERM: return True  # own by others by running
        else: my_error("Unknown error")
    else:
        return True # running

# remote execute related, by default remote execution is off
setup_view_root()
config_dir="%s/integration-test/config" % view_root
remote_config_file="%s/remote_execute_on.cfg" % config_dir
remote_run=False
remote_run_config={}
remote_view_root=None
def get_remote_view_root(): return remote_view_root
def set_remote_view_root(v_root): 
    global remote_view_root
    remote_view_root = v_root
def get_remote_log_dir(): 
    return os.path.join(var_dir_template % remote_view_root, "log")
def get_remote_work_dir():
    #<sst> BUGBUG this needs to be fixed for remote work folders..need single point of truth
    return os.path.join(var_dir_template % remote_view_root, "work")

import ConfigParser

def check_remote_config(remote_config_parser):
  allowed_options=["host","port","view_root"]
  section_names = remote_config_parser.sections() # returns a list of strings of section names
  for section in section_names:
    if not [x for x in components if re.search(x, section)]:
      my_error("Invalid section %s in config file %s" % (section, config))
    if [x for x in ["test_relay, profile_realy, bootstrap_server"] if re.search(x, section)]:
      if not remote_config_parser.has_option(section, "host"):   # set the default host
        remote_config_parser.set(section, "host",host_name_global) 

def parse_config(remote_config_file):
  global remote_run_config
  remote_config_parser = ConfigParser.SafeConfigParser()
  remote_config_parser.read(remote_config_file)
  check_remote_config(remote_config_parser)
  for section in remote_config_parser.sections(): # returns a list of strings of section names
    remote_run_config[section]={}
    for option in remote_config_parser.options(section):
      remote_run_config[section][option]=remote_config_parser.get(section,option)
  #print("remote_config = %s" % remote_config)

if "REMOTE_CONFIG_FILE" in os.environ:   # can be set from env or from a file
  remote_config_file = os.environ["REMOTE_CONFIG_FILE"]
  if not os.path.exists(remote_config_file):  my_error("remote_config_file %s does not exist")
remote_run=os.path.exists(remote_config_file)
if remote_run: remote_config = parse_config(remote_config_file)
 
# url utilities
def quote_json(in_str):
    ret = re.sub('([{,])(\w+)(:)','\\1"\\2"\\3', in_str)
    dbg_print("ret = %s" % ret)
    return ret

def send_url(url_str):
    dbg_print("url_str = %s" % url_str)
    usock = urllib.urlopen(url_str)
    output = usock.read()
    dbg_print("output = %s" % output)
    usock.close()
    return output

# sqlplus
default_db_port=1521
conn_str_template="%%s/%%s@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=%%s)(PORT=%s)))(CONNECT_DATA=(SERVICE_NAME=%%s)))" % default_db_port
sqlplus_cmd="sqlplus"
#sqlplus_cmd="NLS_LANG=_.UTF8 sqlplus"   # handle utf8
sqlplus_heading='''
set echo off
set pages 50000
set long 2000000000
set linesize 5000
column xml format A5000
set colsep ,     
set trimspool on 
set heading off
set headsep off  
set feedback 0
-- set datetime format
ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MON-DD HH24:MI:SS';
ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MON-DD HH24:MI:SS.FF3';
'''
# use milliseconds

def exec_sql_one_row(qry, user, passwd, sid, host):
    return exec_sql(qry, user, passwd, sid, host, True)[0]

def exec_sql_split_results(result_line):
    dbg_print("result_line = %s" % result_line)
    # validate to see if there are errors
    err_pattern = re.compile("ORA-\d+|SP2-\d+")
    is_err=err_pattern.search(result_line)
    if is_err: return [["DBERROR","|".join([r.lstrip() for r in result_line.split("\n") if r != ""])]]
    else: return [[c.strip() for c in r.split(",")] for r in result_line.split("\n") if r != ""]

def exec_sql(qry, user, passwd, sid, host, do_split=False):
    ''' returns an list of results '''
    dbg_print("qry = %s" % (qry)) 
    sqlplus_input="%s \n %s; \n exit \n" % (sqlplus_heading, qry)
    #(user, passwd, sid, host) = tuple(area_conn_info[options.area])
    dbg_print("conn info= %s %s %s %s" % (user, passwd, sid, host))
    sqlplus_call="%s -S %s" % (sqlplus_cmd, conn_str_template % (user, passwd, host, sid))
    os.environ["NLS_LANG"]=".UTF8"  # handle utf8
    ret_str = sys_pipe_call_2(sqlplus_input, sqlplus_call) 
    dbg_print("ret_str = %s" % ret_str)
    # may skip this
    if do_split: return exec_sql_split_results(ret_str)
    else: return ret_str

def parse_db_conf_file(db_config_file, db_src_ids_str=""):
    global db_conn_user_id, db_sid, db_host, db_conn_user_passwd, db_src_info, db_src_ids
    db_src_info={}
    db_sources=json.load(open(db_config_file))
    uri = db_sources["uri"]
    db_conn_user_id = (uri.split("/")[0]).split(":")[-1]
    db_conn_user_passwd = (uri.split("@")[0]).split("/")[-1]
    db_host= (uri.split("@")[1]).split(":")[0]
    tmp = uri.split("@")[1]
    if tmp.find("/") != -1: db_sid = tmp.split("/")[-1]
    else: db_sid = tmp.split(":")[-1]
    dbg_print("db_conn_user_id = %s, db_conn_user_passwd = %s, db_host = %s, db_sid = %s" % (db_conn_user_id, db_conn_user_passwd, db_host, db_sid))

    schema_registry_dir=os.path.join(get_view_root(),"schemas_registry")
    schema_registry_list=os.listdir(schema_registry_dir)
    schema_registry_list.sort()
    sources={}
    for src in db_sources["sources"]: sources[src["id"]]=src
    if db_src_ids_str: 
      if db_src_ids_str=="all": db_src_ids=sources.keys()
      else: db_src_ids = [int(x) for x in db_src_ids_str.split(",")]
    else: db_src_ids=[]
    for src_id in db_src_ids:
      if src_id not in sources: 
        my_error("source id %s not in config file %s. Available source ids are %s" % (src_id, db_config_file, sources.keys()))
      src_info = sources[src_id]
      src_name = src_info["name"].split(".")[-1]
      db_avro_file_path = os.path.join(schema_registry_dir,[x for x in schema_registry_list if re.search("%s.*avsc" % src_name,x)][-1])
      if not os.path.exists(db_avro_file_path): my_error("Schema file %s does not exist" % db_avro_file_path)
      db_user_id = db_conn_user_id
      src_uri = src_info["uri"]
      if src_uri.find(".") != -1: db_user_id = src_uri.split(".")[0]
      db_src_info[src_id] = {"src_name":src_name,"db_user_id":db_user_id, "db_avro_file_path":db_avro_file_path, "uri":src_uri}
      dbg_print("db_src_info for src_id %s = %s" % (src_id, db_src_info[src_id]))
    return db_conn_user_id, db_sid, db_host, db_conn_user_passwd, db_src_info, db_src_ids

''' mysql related stuff '''
mysql_cmd="mysql"
def get_mysql_call(dbname, user, passwd, host):
    conn_str = "%s -s " % mysql_cmd
    if dbname: conn_str += "-D%s " % dbname
    if user: conn_str += "-u%s " % user
    if passwd: conn_str += "-p%s " % passwd
    if host: conn_str += "-P%s " % host
    return conn_str

def mysql_exec_sql_one_row(qry, dbname=None, user=None, passwd=None, host=None):
    ret = mysql_exec_sql(qry, dbname, user, passwd, host, True)
    dbg_print("ret = %s" % ret)
    if ret: return ret[0]
    else: return None

def mysql_exec_sql_split_results(result_line):
    dbg_print("result_line = %s" % result_line)
    # validate to see if there are errors
    err_pattern = re.compile("ERROR \d+")
    is_err=err_pattern.search(result_line)
    if is_err: return [["DBERROR","|".join([r.lstrip() for r in result_line.split("\n") if r != ""])]]
    else: return [[c.strip() for c in r.split("\t")] for r in result_line.split("\n") if r != ""]

def mysql_exec_sql(qry, dbname=None, user=None, passwd=None, host=None, do_split=False):
    ''' returns an list of results '''
    dbg_print("qry = %s" % (qry)) 
    mysql_input=" %s; \n exit \n" % (qry)
    dbg_print("conn info= %s %s %s %s" % (dbname, user, passwd, host))
    mysql_call=get_mysql_call(dbname, user, passwd, host)
    dbg_print("mysql_call= %s" % (mysql_call))
    #if not re.search("select",qry): return None # test only, select only
    ret_str = sys_pipe_call_21(mysql_input, mysql_call)   # also returns the error
    dbg_print("ret_str = %s" % ret_str)
    # may skip this
    if do_split: return mysql_exec_sql_split_results(ret_str)
    else: return ret_str

def get_copy_name(input_file_name):
    input_f = os.path.basename(input_file_name)
    input_f_split =  input_f.split(".")
    append_idx = min(len(input_f_split)-2,0)
    input_f_split[append_idx] += time.strftime('_%y%m%d_%H%M%S')
    new_file= os.path.join(work_dir, ".".join(input_f_split))
    return new_file

def save_copy(input_files):
    for i in range(len(input_files)):
      new_file= get_copy_name(input_files[i])
      dbg_print("Copy %s to %s" % (input_files[i], new_file))
      if not remote_run:
        shutil.copy(input_files[i], new_file)
      else:
        remote_run_copy(input_files[i], new_file, i)
      input_files[i] = new_file
    return input_files

def save_copy_one(input_file):
    ''' wrapper for save copy '''
    input_files=[input_file]
    save_copy(input_files)
    return input_files[0]

def db_config_detect_host_nomral_open(db_host, db_port, db_user=None, passwd=None, db_sid=None):
    return isOpen(db_host, db_port)

def db_config_detect_host_oracle_open(db_host, db_port, db_user, passwd, db_sid):
    ret = exec_sql("exit", db_user, passwd, db_sid, db_host, do_split=False)
    return not re.search("ERROR:",ret)

def db_config_detect_host(db_host, db_port=default_db_port, detect_oracle=False, db_user=None, passwd=None, db_sid=None):
    detect_func = detect_oracle and db_config_detect_host_oracle_open or db_config_detect_host_nomral_open
    if detect_func(db_host, db_port, db_user, passwd, db_sid): return (db_host, db_port)  # OK
    possible_hosts = ["localhost"]        # try local host
    found_host = False
    for new_db_host in possible_hosts:
      if not detect_func(new_db_host, db_port, db_user, passwd, db_sid): continue
      found_host = True
      break
    if not found_host: my_error("db server on %s and possible hosts %s port %s is down" % (db_host, possible_hosts, db_port))
    print "Substitue the host %s with %s" % (db_host, new_db_host)
    return (new_db_host, db_port)

def db_config_change(db_relay_configs):
    new_db_config_files = []
    print "db_config_change: ", db_relay_configs.split(',')
    for db_relay_config in db_relay_configs.split(','):
        print "db_config: ", db_relay_config
        ''' if there is a config file, handle the case that db is on on local host '''
        (db_conn_user_id, db_sid, db_host, db_conn_user_passwd, db_src_info, db_src_ids) = parse_db_conf_file(db_relay_config)
        (new_db_host, new_db_port) = db_config_detect_host(db_host, detect_oracle=True, db_user=db_conn_user_id, passwd=db_conn_user_passwd, db_sid=db_sid)
        if new_db_host == db_host:
            new_db_config_files.append(db_relay_config) 
            continue
        new_db_config_file = get_copy_name(db_relay_config)
        print "New config file is %s" % (new_db_config_file)
        host_port_re = re.compile("@%s:%s:" % (db_host, default_db_port))
        new_host_port = "@%s:%s:" % (new_db_host, new_db_port)
        new_db_config_f = open(new_db_config_file, "w")
        for line in open(db_relay_config):
          new_db_config_f.write("%s" % host_port_re.sub(new_host_port, line))
          
        new_db_config_files.append(new_db_config_file)
    return ",".join(new_db_config_files)

# get a certain field in url response
def http_get_field(url_template, host, port, field_name, extractor=None):
    
    out = send_url(url_template % (host, port)).split("\n")[1]
    if re.search("Exception:", out): my_error("Exception getting: %s" % out)
    # work around the invalid json, with out the quote, DDS-379
    out2=quote_json(out)
    field_value = json.loads(out2)
     
    retVal = -1
    try:
        if (extractor is None):
            retVal = field_value[field_name]
        else:
            retVal = extractor(field_value, field_name)
    except KeyError:
        dbg_print("Response was :" + str(field_value) )
        dbg_print("Field name is :" + field_name)
        for f in field_value.iterkeys():
            dbg_print("Key is :" + str(f))                
        raise
        
    return retVal

# wait util
# Classes
class RetCode:
  OK=0
  ERROR=1
  TIMEOUT=2
  DIFF=3
  ZERO_SIZE=4
  
# wait utility
def wait_for_condition_1(cond_func, timeout=60, sleep_interval = 0.1):
  ''' wait for a certain cond. cond could be a function. 
     This cannot be in utility. Because it needs to see the cond function '''
  #dbg_print("cond = %s" % cond)
  if sys_call_debug: return RetCode.OK
  sleep_cnt = 0
  ret = RetCode.TIMEOUT
  while (sleep_cnt * sleep_interval < timeout):
    dbg_print("attempt %s " % sleep_cnt)
    if cond_func():
      dbg_print("success")
      ret = RetCode.OK
      break
    time.sleep(sleep_interval)
    sleep_cnt += 1
  return ret

# helper utility functions
def convert_range_to_list(range_str):
    #converts input such as 0-3;7 to list [0,1,2,3,7]
    result = []
    for part in range_str.split(';'):
        if '-' in part:
            a, b = part.split('-')
            a, b = int(a), int(b)
            result.extend(range(a, b + 1))
        else:
            a = int(part)
            result.append(a)
    return result

#====End of Utilties============

