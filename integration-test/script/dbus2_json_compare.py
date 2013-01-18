#!/usr/bin/env python

'''
  Compare json event files flag any difference
  Can also do the apply or compress which keep the last key
  
  Services: test_relay, test_bootstrap_producer
  In a configure file, then we can have,  bootstrap_consumer1

  make a copy
  test_relay 1  machine_name port memory =>  which process  (available)
  test_relay 2  machine_name port memory etc
  test_bootstrap_producers  machine_name
  bootstrap_server  machine_name port memory etc


config => Remote setup, create view, etc
=> run the test, with remote annotation
   

'''
__author__ = "David Zhang"
__version__ = "$Revision: 0.1 $"
__date__ = "$Date: 2010/11/16 $"

import sys, os
import pdb
import time, copy, re
from optparse import OptionParser, OptionGroup
#import threading
import urllib
from utility import *
import shutil
from xml.etree.ElementTree import iterparse
      
# Global varaibles
options=None
host_name_global = (os.popen("/bin/hostname").read()).split(".")[0]
my_pid = os.getpid()
server_host="localhost"
server_port="8080"
outf = sys.stdout # output file handle, default to stdout
fk_src_id_order=[]
fk_src_id_order_index={}  # look up list from src id to seq sequence 20,21 -> 0, 1
fk_src_id_state=[]   #  for each file,  {{src:"maxSeq", key:},"ErrorType":"src out of seq", "event out of order""Error":{src 2 get seq, key > seq 1 max seq, src 1 get seq, key < src 1 max seq }, error type, 

def KeyRangeFilter_flatten_partitions(partition):
  ''' given a partition like [1,3-5], return a list[1,3,4,5] '''
  flattened_partitions=[]
  for x in partition.rstrip("]").lstrip("[").split(","):
    if "-" in x: 
      start, end = x.split("-")
      flattened_partitions.extend([long(x) for x in range(long(start), long(end)+1)])
    else:
      flattened_partitions.append(long(x))
  dbg_print("flattened_partitions = %s " % flattened_partitions)
  return flattened_partitions
  
def KeyRangeFilter(keyMin, keyMax, server_side_filter):
  if keyMin and keyMax:
    return lambda x: x["key"] >= keyMin and x["key"] < keyMax
  if server_side_filter:
    filter_dict = {}
    for x in server_side_filter.split(";"): 
      pair = x.split("=") 
      if len(pair) !=2: my_error("Wrong input in server_side_filter: %s" % x)
      filter_dict[pair[0]] = pair[1]
    dbg_print("filter_dict = %s" % filter_dict)
    key_str = "key" not in filter_dict and "key" or filter_dict["key"]
    if filter_dict["type"] == "range":
      range_size = long(filter_dict["range.size"])
      range_partitions = KeyRangeFilter_flatten_partitions(filter_dict["range.partitions"])
      return lambda x: long(x[key_str])/range_size in range_partitions
    if filter_dict["type"] == "mod":
      mod_numBuckets = long(filter_dict["mod.numBuckets"])
      mod_buckets = KeyRangeFilter_flatten_partitions(filter_dict["mod.buckets"])
      def lambda_1(x):
       # if long(x[key_str])==9996: 
       #   print "key_str=%s, x[key_str] =%s, mod_numBuckets=%s, mod_buckets=%s" % (key_str, x[key_str] , mod_numBuckets, mod_buckets)
        return long(x[key_str])%long(mod_numBuckets) in mod_buckets
      return lambda_1

# iterator class
class Events:
  def __init__(self, in1, in2, filterFunc, filterConsumer=False, offset1=0, offset2=0):
    self.in1 = in1
    self.in2 = in2
    self.filterFunc1 = filterFunc
    self.filterFunc2 = filterConsumer and filterFunc or None
    self.isList = isinstance(in1, list)
    if (self.isList):
      self.in1_ind = offset1
      self.in2_ind = offset2
      self.in1_len = len(in1)
      self.in2_len = len(in2)
    else:
      if ( not offset1):
        offset1=0
      if ( not offset2):
        offset2=0

      for i in range(int(offset1)):
        get_next_event(self.in1, self.filterFunc1, 0)
      for j in range(int(offset2)):
        get_next_event(self.in2, self.filterFunc2, 1)
    
  def __iter__(self):
      return self
  def next(self):
    ret1 = ""
    ret2 = ""
    if (self.isList):
      if (self.in1_ind < self.in1_len): 
	ret1 = self.in1[self.in1_ind]
        notFilteredRows = True
	if self.filterFunc1:
          while (not self.filterFunc1(ret1)):
            notFilteredRows = False
            if self.in1_ind >= self.in1_len : break;
            ret1 = self.in1[self.in1_ind] 
            self.in1_ind += 1 
          if (not self.filterFunc1(ret1) and self.in1_ind >= self.in1_len): ret1 ="" 
        if notFilteredRows:
         self.in1_ind += 1
      if (self.in2_ind < self.in2_len): 
	ret2 = self.in2[self.in2_ind] 
        notFilteredRows = True
        if self.filterFunc2:
          while (not self.filterFunc2(ret2)):
            notFilteredRows = False
            if self.in2_ind >= self.in2_len : break;
	    ret2 = self.in2[self.in2_ind] 
            self.in2_ind += 1
          if (not self.filterFunc2(ret2) and self.in2_ind >= self.in2_len): ret2 ="" 
        if notFilteredRows:
         self.in2_ind += 1
    else:
      ret1 = get_next_event(self.in1, self.filterFunc1, 0)
      ret2 = get_next_event(self.in2, self.filterFunc2, 1)  # filter is only applied on the source
    return (ret1, ret2)
	
# functions
def setup_remote_run_find_section(search_str):
    for section in remote_run_config:
      if re.search(search_str, section): 
        ret_section=section
        break
    return ret_section

remote_section={}
def setup_remote_run():
    remote_section[0] = setup_remote_run_find_section("_relay")
    # TODO: Need to handle multiple consumers
    remote_section[1] = setup_remote_run_find_section("_consumer")
    global options
    options.save_copy = True  # save copy during remote run
   
def setup_env():
    setup_work_dir()
    if remote_run: setup_remote_run()

#print_key_orders=["key","scn","windowScn","partitionId","timestamp","srcId","schemaId","valueEnc","endOfPeriod","value"]
print_key_orders=["key","sequence","logicalPartitionId","timestampInNanos","srcId","schemaId","valueEnc","endOfPeriod","value"]


class EventEncoder(json.JSONEncoder):
    ''' print the json in order 
{"key":0,"sequence":0,"logicalPartitionId":0,"timestampInNanos":1291005065490,"srcId":-2,"schemaId":"AAAAAAAAAAAAAAAAAAAAAA==","valueEnc":"JSON","endOfPeriod":true,"value":"ZW1wdHlWYWx1ZQ=="}
    '''
    def add_one(self, k, obj, first):
      if isinstance(obj[k], str) or isinstance(obj[k], unicode): quote='"' 
      else: quote=''
      comma=","
      if first: comma=""
      ret = '%s"%s":%s%s%s' % (comma, k, quote, obj[k], quote)
      return ret.replace("\n","\\n")
    def encode(self, obj):
      if not isinstance(obj,dict): return json.JSONEncoder.encode(self,obj)
      first=True
      ret = "{"
      keys = obj.keys()
      for k in print_key_orders: 
        if k in keys: 
          ret +=self.add_one(k, obj, first)
          first = False
          keys.remove(k)
      for k in keys: 
          ret +=self.add_one(k, obj, first)
          first = False
      ret += "}"
      return ret

def deep_diff_print_detail(level, e1, e2):
    outf.write("value difference for key [%s]: \n <<<<<<<<<<\n %s \n >>>>>>>>>> \n %s \n" % (level, e1, e2))

def deep_diff_detail(level, e1, e2):
    dbg_print("\nlevel = %s" % level)
    for k in e1:
      if k not in e2: 
        outf.write("key [%s] is in file1 but not in file2\n" % k)
        deep_diff_print_detail(level, e1, e2)
        return RetCode.DIFF
    if not e2: pass #pdb.set_trace()
    for k in e2:
      if k not in e1: 
        outf.write("key [%s] is in file2 but not in file1\n" % k)
        deep_diff_print_detail(level, e1, e2)
        return RetCode.DIFF
    for k in e1:
      dbg_print("\nkey = %s" % k)
      if isinstance(e1[k], dict): 
         ret = deep_diff_detail("%s:%s" % (level, k), e1[k], e2[k])
         if ret == RetCode.DIFF: return ret
         else: continue
      if isinstance(e1[k], list): 
        for i in range(len(e1[k])):
          item1 = e1[k][i]
          item2 = e2[k][i]
          ret = deep_diff_detail("%s:%s:%s" % (level, k, i), item1, item2)
          if ret == RetCode.DIFF: return ret
          else: continue
      else:
        if e1[k] != e2[k]: 
          #outf.write("value difference for key [%s]: %s / %s " % ("%s:%s" % (level, k), e1[k], e2[k]))
          deep_diff_print_detail("%s:%s" % (level, k), e1[k], e2[k])
          if k == "long" and long(e1[k])/1000 == long(e2[k])/1000: 
            outf.write("Long Masked\n")
            #pdb.set_trace()
            return RetCode.OK  # hack for the date time issue
          if k == "string" and (e1[k].find("\n") != -1 or e2[k].find("\n") != -1):
            val1 = e1[k].replace("\n","")
            val2 = e2[k].replace("\n","")
            if val1 == val2: 
               outf.write("\\n Masked\n")
               return RetCode.OK  # hack for the date time issue
          return RetCode.DIFF
    return RetCode.OK
    
def deep_diff(e1, e2, event_count):
    ''' print detailed diff '''
    outf.write("Data Difference in two files")
    outf.write("==========\n")
    outf.write("event_count = %s\n" % event_count)
    if (e1): 
      outf.write("<<<<<<<<<<<\n")
      #outf.write("%s\n" % e1)
      outf.write("%s\n" % json.dumps(e1, cls=EventEncoder))
    if (e2): 
      outf.write(">>>>>>>>>>>\n")
      #outf.write("%s\n" % e2)
      outf.write("%s\n" % json.dumps(e2, cls=EventEncoder))
    outf.write("\n==========\n")
    if (e1 and e2): return deep_diff_detail("ROOT", e1, e2)
    outf.write("==========\n")

total_line_no=0
global_start_time=time.time()
#json_line_reg=re.compile('"(key)":(\d+)\S+"(sequence)":(\d+)\S+"(srcId)":(\d+)')
json_line_reg=re.compile('"(key|sequence|srcId)":(\d+)\S+"(key|sequence|srcId)":(\d+)\S+"(key|sequence|srcId)":(\d+)')
def get_next_event(f, filterFunc=None, file_id=0):
    ''' get next no empty line and convert to json  '''
    line_no = 0
    while (True):
      line = f.readline()
      #print("line=%s" % line)
      if not line: return None
      line_no = line_no + 1
      try: 
        if line!="\n": 
          if options.fk_src_order_only:   # regex match
            json_line={}
            m = json_line_reg.search(line)
            if not m: my_error("No key, sequence, or srcID. line = " + line)
            else: 
              for i in (1,3,5): json_line[m.group(i)] = int(m.group(i+1)) 
          else:
            if not options.no_json_conversion:
               try: json_line = json.loads(line)
               except ValueError: 
                 if options.ignore_json_error: continue  # ignore the value error
                 else: raise
            else: json_line=line.rstrip()
            #print("json_line=%s" % json_line)
          if filterFunc:
            if (filterFunc(json_line)==True):
              dbg_print("Passed filter key = %s" % json_line["key"])
              break
            else:
              continue
          else:
            break
      except:
        print "f= %s line_no = %s, line = %s " % (f, line_no, line)
        raise
    global total_line_no, global_start_time
    total_line_no += line_no
    if total_line_no % 100000 == 0: print "1:total_1ine_no = %s, time=%s" % (total_line_no, time.time() - global_start_time)
    if options.fk_src_order: # check src id sequence
      global fk_src_id_state
      src_id = json_line["srcId"]
      seq = long(json_line["sequence"])
      if src_id in fk_src_id_order:   # this is needed because may just check some srcs
        if seq < fk_src_id_state[file_id][src_id]["maxSeq"]:
          if "Error" not in fk_src_id_state[file_id][src_id]:
            fk_src_id_state[file_id][src_id]["Error"] = "Src %d sequence Error: seq=%s key=%s < maxSeq=%s" % (src_id, seq, json_line["key"], fk_src_id_state[file_id][src_id]["maxSeq"])
        else:
          fk_src_id_state[file_id][src_id]["maxSeq"] = seq
          #print "new maxSeq = %s" % seq
        # search from pk side, any seem before is wrong
        for fk_src_id in fk_src_id_order[fk_src_id_order_index[src_id]+1:]:  # all the fks
          if seq < fk_src_id_state[file_id][fk_src_id]["maxSeq"]:    # same window source order should hold
            if "Error" not in fk_src_id_state[file_id][src_id]:
              fk_src_id_state[file_id][src_id]["Error"] = "PK-FK sequence Error: srcId=%s, maxSeq=%s appears before pkSrcID = %s seq=%s key=%s " % (fk_src_id, fk_src_id_state[file_id][fk_src_id]["maxSeq"], src_id, seq, json_line["key"],)
    #if total_line_no % 10000 == 0: print "2:total_1ine_no = %s, time=%s" % (total_line_no, time.time())
    return json_line

def check_sequence(inputf):
    has_line = True
    while (has_line): has_line = get_next_event(inputf, None, 0)  # loop through the lines

def straight_compare(f1, f2, filterFunc=None, offset1=0, offset2=0):
    ''' given two files. compare and return the difference. 
        return True if diff and False otherwise
    '''
    events =  Events(f1, f2, filterFunc, options.enable_consumer_key_range, offset1, offset2)
    zero_size = True
    event_count = 0
    for e1, e2 in events:
      if e1 and e2 and options.ignored_columns:
         for col in options.ignored_columns.split(","):
           #print "col %s" %col 
           if col in e1: del e1[col]
           if col in e2: del e2[col]
      event_count += 1
      if (e1 != e2): 
        ret = deep_diff(e1, e2, event_count)
        if ret == RetCode.OK:
           print "==OK after adjusting to long"  # work around the date issue
           continue
        return RetCode.DIFF
      if (not e1 and not e2): break
      zero_size = False
    if zero_size: return RetCode.ZERO_SIZE
    return RetCode.OK

def compress_input_1(fname):
    compress_input(open(fname))

def compress_input(f, file_id=0):
    ''' given a file. return a compressed list '''
    source_dict={}
    while (True):
      e = get_next_event(f, None, file_id)
      if not e: break
      if "srcId" in e: src_id = e["srcId"]
      if src_id not in source_dict: source_dict[src_id]={"compressed_events":{},"key_order":[]}

      if "key" in e: key = e["key"] 
      elif "id" in e: key = e["id"] 
      elif "keyBytes" in e: key = e["keyBytes"]
      elif "value" in e: 
        e1 = e["value"]
        if "key" in e1: key = e1["key"] 
        elif "id" in e1: key = e1["id"]
        elif "keyBytes" in e1: key = e1["keyBytes"]
        else: my_error("Event does not have key or id field. %s" % e) 
      else: my_error("Event does not have key or id field. %s" % e) 

      if isinstance(key, dict):  
        key = key.values()[-1]  # get the last value
        #key = key["int"]
      try: 
        if not options.compress_key_only and key not in source_dict[src_id]["compressed_events"]: 
          source_dict[src_id]["key_order"].append(key)
      except: raise
      source_dict[src_id]["compressed_events"][key] = options.compress_key_only and 1 or  e
    if source_dict:
      sources = source_dict.keys()   # sort based on source id
      sources.sort()
    else: return []   # no input
    ret = []
    for src_id in sources:  # order sequence
      if options.sort_key: source_dict[src_id]["key_order"].sort()
      if not options.compress_key_only:
        ret.extend([source_dict[src_id]["compressed_events"][k] for k in source_dict[src_id]["key_order"]])
      else:
        ret.extend(source_dict[src_id]["compressed_events"].keys())
    return ret

def compress_compare(f1, f2, filterFunc, offset1=0, offset2=0):
    ''' Compare files after compress '''
    c1 = compress_input(f1, 0) 
    c2 = compress_input(f2, 1) 
    return straight_compare(c1, c2, filterFunc, offset1, offset2)

def do_compare(fname1, fname2, filterFunc, offset1=0, offset2=0):

    if ( not offset1):
       offset1=0
    if ( not offset2):
       offset2=0

    print "comparing: dbus2_json_compare.py %s %s with offsets %s and %s" % (fname1, fname2, str(offset1), str(offset2))
    f1 = open(fname1)
    f2 = open(fname2)

    if options.compress: return compress_compare(f1, f2, filterFunc, offset1, offset2)
    else: return straight_compare(f1, f2, filterFunc, offset1, offset2)

def check_exists(input_files):
    ret_files=[]
    for input_f in input_files:
      ret_file = file_exists(input_f) 
      if not ret_file: 
        my_error("Input file %s does not exist" % input_f)
      ret_files.append(ret_file)
    dbg_print("ret_files=%s" % ret_files)
    return ret_files

remote_copy_cmd="scp %s:%s %s"
def remote_run_copy(from_file, to_file, i):
    # convert from file to a remote file
    sys_call("scp %s:%s %s" % \
             (remote_run_config[remote_section[i]]["host"], \
             os.path.join(remote_run_config[remote_section[i]]["view_root"],from_file), \
             to_file))

db_json_dump_file=None  # one json file
db_dump_batch_size=20
db_conn_user_id = None
db_conn_user_passwd = None
db_sid=None
db_host=None
db_src_info={}
db_src_ids=None  # list of int of source ids

# Need to handle multiple sources
# src id
def db_generate_json(): 
    db_generate_json_setup()
    db_get_meta_data()
    for src_id in db_src_ids:
      db_xml_dump_file=os.path.join(get_work_dir(), "%s_%s.xml" % (db_src_info[src_id]["src_name"], time.strftime('%y%m%d_%H%M%S')))
      dbg_print("db_xml_dump_file=%s" % (db_xml_dump_file))
      (min_txn, max_txn) = db_get_txn_range(src_id)
      dbg_print("min_txn = %s, max_txn = %s" % (min_txn, max_txn))
      # TODO: remove
      if options.db_xml_dump_file:
        db_xml_dump_file=options.db_xml_dump_file
      else:
        db_generate_xml_dump_batch(src_id, db_xml_dump_file, long(min_txn), long(max_txn))
      convert_xml_to_json(src_id, db_xml_dump_file, db_json_dump_file)

def db_get_txn_range(src_id): 
    db_user_id = db_src_info[src_id]["db_user_id"]
    #db_table_name = db_src_info[src_id]["db_table_name"]
    scn_cond = options.from_scn and "where scn > %s and ora_rowscn > %s" % (options.from_scn, options.from_scn) or ""
    qry = "select min(txn), max(txn) from %s.sy$txlog %s" % (db_user_id, scn_cond)
    return tuple(exec_sql_one_row(qry, db_conn_user_id, db_conn_user_passwd, db_sid, db_host))

def db_generate_xml_dump_batch(src_id, db_xml_dump_file, min_txn, max_txn):
    db_user_id = db_src_info[src_id]["db_user_id"]
    db_table_name = db_src_info[src_id]["db_table_name"]
    src_name = db_src_info[src_id]["src_name"]
    xml_dump_file = open(db_xml_dump_file,"w")
    xml_dump_file.write('''<?xml version="1.0"?>\n<ROWSET>\n''')
    start_txn = min_txn
    #start_of_file=True
    while (start_txn <= max_txn):
      end_txn = min(start_txn + db_dump_batch_size, max_txn)
      #qry = "select DBMS_xmlgen.GETXML('SELECT * from %s.%s where key>=%s and key<=%s') xml from dual" % (db_user_id, db_table_name, start_key, end_key)
      #qry = "select DBMS_xmlgen.GETXML('select /*+ first_rows LEADING(tx) */ %s.sync_core.getScn(tx.scn, tx.ora_rowscn) scn, tx.ts event_timestamp, src.* from %s.ile src, member2.sy$txlog tx where src.txn=tx.txn and tx.scn > %s and tx.ora_rowscn > %s
      qry = "select get_xml_null('SELECT * from %s.%s where txn>=%s and txn<=%s order by txn') xml from dual" % (db_user_id, db_table_name, start_txn, end_txn)
      #qry = "select DBMS_xmlgen.GETXML('SELECT * from %s.%s where txn>=%s and txn<=%s order by txn') xml from dual" % (db_user_id, db_table_name, start_txn, end_txn)
      #qry = "select DBMS_xmlgen.GETXML('select /*+ first_rows LEADING(tx) */ %s.sync_core.getScn(tx.scn, tx.ora_rowscn) scn, tx.ts event_timestamp, src.* from %s.%s src, %s.sy$txlog tx where src.txn=tx.txn and tx.txn >= %s and tx.txn <=%s') xml from dual" % (db_user_id, db_user_id, db_user_id, db_table_name, start_txn, end_txn)
      #qry = "select DBMS_xmlgen.GETXML('select  */ %s.sync_core.getScn(tx.scn, tx.ora_rowscn) scn, tx.ts event_timestamp, src.* from %s.%s src, %s.sy$txlog tx where src.txn=tx.txn and tx.txn >= %s and tx.txn <=%s') xml from dual" % (db_user_id, db_user_id, db_user_id, db_table_name, start_txn, end_txn)
      ret = exec_sql(qry, db_conn_user_id, db_conn_user_passwd, db_sid, db_host)
      #if start_of_file: # get rid of t
      #  start_of_file = False
      #  ret = "\n".join([x for x in ret.split("\n") if x])
      out_lines = []
      out_line = ""
      for line in ret.split("\n")[2:-1]:
        if not line: continue
        out_line += line
        if line[-1] == ">":
          xml_dump_file.write(out_line+"\n")
          out_line = ""
      start_txn = end_txn + 1
    xml_dump_file.write('''\n</ROWSET>\n''')
    xml_dump_file.close()

def db_create_xml_dump_function():
    func_qry = '''
create or replace function get_xml_null(qry varchar2) return clob is
  qryCtx   DBMS_XMLGEN.ctxHandle;
begin
  qryCtx := dbms_xmlgen.newContext(qry);
  DBMS_XMLGEN.setNullHandling(qryCtx, dbms_xmlgen.EMPTY_TAG); 
  return DBMS_XMLGEN.getXML(qryCtx);
end;
/
'''
    ret = exec_sql(func_qry, db_conn_user_id, db_conn_user_passwd, db_sid, db_host)

def db_get_conn_info():
    global db_conn_user_id, db_sid, db_host, db_conn_user_passwd, db_src_info, db_src_ids
    (db_conn_user_id, db_sid, db_host, db_conn_user_passwd, db_src_info, db_src_ids) = parse_db_conf_file(options.db_config_file, options.db_src_ids)
    (db_host, db_port) = db_config_detect_host(db_host, detect_oracle=True, db_user=db_conn_user_id, passwd=db_conn_user_passwd, db_sid=db_sid)

def db_generate_json_setup():
    db_get_conn_info()
    db_create_xml_dump_function()
    db_get_meta_data()
    global db_json_dump_file
    src_names = []
    for src_id in db_src_ids: src_names.append(db_src_info[src_id]["src_name"])
    db_json_dump_file=os.path.join(get_work_dir(), "%s_%s.json" % ("_".join(src_names), time.strftime('%y%m%d_%H%M%S')))
    dbg_print("db_json_dump_file=%s" % (db_json_dump_file))

def db_get_meta_data_process_fields(fields, parent_field_name, db_array_types, db_field_type_info):
    for f in fields:
      if not isinstance(f["type"], dict):  # not array
        db_col_name = f["meta"].split(";")[0].split("=")[-1]
        db_field_type_info["%s;%s" % (parent_field_name,db_col_name)]=[f["type"][0], f["name"]]
      else: 
        items = f["type"]["items"] 
        db_col_name = items["meta"].split(";")[0].split("=")[-1]
        db_array_types.append(db_col_name)
        db_field_type_info["%s;%s" % (parent_field_name,db_col_name)]=[f["type"]["type"], f["name"]]
        db_get_meta_data_process_fields(items["fields"], "%s;%s" % (parent_field_name, db_col_name), db_array_types, db_field_type_info)
    
def db_get_meta_data():
    ''' get meta data from the avro file '''
    for src_id in db_src_ids:
      schema = json.load(open(db_src_info[src_id]["db_avro_file_path"]))
      #dbg_print("schema = %s" % schema)
      db_table_name = schema["meta"].split("=")[-1].split(";")[0]
      db_array_types=[]
      db_field_type_info={}
      db_get_meta_data_process_fields(schema["fields"],db_src_info[src_id]["src_name"], db_array_types, db_field_type_info)
      dbg_print("db_array_types = %s \n db_field_type_info = %s" % (db_array_types, db_field_type_info))
      db_src_info[src_id]["db_array_types"]=db_array_types
      db_src_info[src_id]["db_field_type_info"]=db_field_type_info
      db_src_info[src_id]["db_table_name"]=db_table_name
#    db_array_types=['PROF_POSITIONS','PROF_EDUCATIONS','PROF_ELEMENTS']
#    db_field_types=['PROF_POSITIONS','PROF_EDUCATIONS','PROF_ELEMENTS']

convert_xml_db_field_type_info=None
def get_elem_type_name(levels_combine, tag):
    ''' use meta data from the avro file to figure out type'''
    return tuple(convert_xml_db_field_type_info["%s;%s" % (levels_combine, tag)])

def convert_xml_to_json_escape_text(text, elem_tag, elem_type):
    ''' do the escape of \n '''
    ret = text
    if elem_type == "string" and text:
      ret = text.replace('"','\\"')
      #ret = ret.replace('\\','\\\\')
      #ret = ret.replace('\n','\\n"')
      ret = ret.replace('\n','\\n')
      ret = '"%s"' % ret
    #if elem_type == "long" and elem_tag.split("_")[-1] == "DATE":
    if elem_type == "long" and text and elem_tag.split("_")[-1] != "ID":
      #pdb.set_trace()
      try: 
        text_split = text.split(".")
        t_struct = time.strptime(text_split[0], "%Y-%b-%d %H:%M:%S")
        # looks like the modified_date in member_profile will not get the millisecs but the modified_date ROOT:profElements will get it
        ret = long(1000*time.mktime(t_struct))
        #if (len(text_split)>1): ret += long(text_split[1])  # add back the milliseconds
      except ValueError: pass
    if ret == None: ret = "null"
    else: ret = '{"%s":%s}' % (elem_type, ret)
    return ret

# TODO fix DATE
def convert_xml_to_json(src_id, xml_file_name, json_file_name):
    global convert_xml_db_field_type_info
    curly_brackets = ['{','}']
    squre_brackets = ['[',']']
    json_outf=open(json_file_name, "a")  # append
    src_info = db_src_info[src_id]
    convert_xml_db_field_type_info=src_info["db_field_type_info"]
    db_array_types = src_info["db_array_types"]
    levels=[src_info["src_name"]]
    record_start = True
    array_start = True
    levels_combine = levels[0]
    #pdb.set_trace()
    for event, elem in iterparse(xml_file_name, events=("start", "end")):
      level = levels[-1]
      #print "level = %s" % level
      if event == "start":
          #print "start"
          #print elem.tag
          if elem.tag == 'ROWSET': pass
          elif elem.tag == 'ROW' or (elem.tag[-2:]=='_T' and level != 'ROW'):
            if elem.tag != 'ROW': # type
              if not record_start and not array_start: json_outf.write(",")
              array_start = False
            # record start
            if elem.tag == 'ROW': json_outf.write('{"srcId":%s,"value":{' % src_id)
            else: json_outf.write(curly_brackets[0])
            record_start=True
          else:
            if not record_start: json_outf.write(",")
            else: record_start = False
            if elem.tag in db_array_types:
              elem_type, elem_name = get_elem_type_name(levels_combine, elem.tag)
              levels.append(elem.tag)
              levels_combine = ";".join(levels)
              json_outf.write('"%s":%s' % (elem_name, squre_brackets[0]))
              array_start=True
      else: # end tag
          if elem.tag == 'ROWSET': pass
          elif elem.tag == 'ROW' or (elem.tag[-2:]=='_T' and level != 'ROW'):
            # end of record
            if elem.tag == 'ROW': json_outf.write(curly_brackets[1]+curly_brackets[1])
            else: json_outf.write(curly_brackets[1])
            if elem.tag == 'ROW': 
              json_outf.write("\n")
              #elem.clear() # delete the elem after processing to save space 
          elif elem.tag in db_array_types:
            json_outf.write(squre_brackets[1])
            levels.pop()
            levels_combine = ";".join(levels)
          elif not elem.tag in db_array_types:
            elem_type, elem_name = get_elem_type_name(levels_combine, elem.tag)
            #json_outf.write('"%s":{"%s":"%s"}' % (elem_name, elem_type, convert_xml_to_json_escape_text(elem.text, elem.tag, elem_type)))
            try: 
              out_txt = convert_xml_to_json_escape_text(elem.text, elem.tag, elem_type)
              #json_outf.write(u'"%s":{%s}' % (elem_name,out_txt.encode('utf-8')))     # why we do this?, some uft-9 issues?
              #
              #json_outf.write('"%s":{' % (elem_name))
              #json_outf.write(out_txt.encode('utf-8'))
              #json_outf.write('}')
              json_outf.write('"%s":' % (elem_name))
              json_outf.write(out_txt.encode('utf-8'))

              #json_outf.write(u'"%s":{%s}' % (elem_name,out_txt.encode('utf-8')))
              #json_outf.write('"%s":{%s}' % (elem_name,out_txt))
            #except UnicodeEncodeError, e: pdb.set_trace()
            except UnicodeEncodeError, e: raise
    json_outf.close()
def espresso_value_compare_line( ipstring, opstring ):
    retValue = True
    dbg_print("ipstring: %s" % (ipstring))
    dbg_print("opstring: %s" % (opstring))

    ipjson = json.loads(ipstring)
    opjson = json.loads(opstring)
    dbg_print ("Comparing" + ipstring + "with" + opstring)
    if ipjson["id"] != opjson["value"]["id"] :
        retValue = False
    if ipjson["name"] != opjson["value"]["name"] :
        retValue = False
    return retValue

def espresso_value_compare_partition(db_name, partition_num, data_file, consumer_value_dump_file):
    global options
    retCode = RetCode.OK
    cmd = ("curl -I -s http://%s:%s/%s/IdNamePair/%%s"%(options.espresso_host, options.espresso_port, db_name))
    if not (options.espresso_key_range) :my_error("key range not specified")
    if not (options.espresso_host) :my_error("espresso hostname not specified")
    if not (options.espresso_port) :my_error("espresso portname not specified")
    fipfile = open (data_file, 'r')
    fcsmrfile = open (consumer_value_dump_file, 'r')
    partition_name = db_name+"_"+partition_num
    # read in the input data and put it in memory 
    ip_values=[] 
    count = 0
    num_data = int(options.espresso_key_range)
    done = False
    while not done:
        for line in fipfile:
            ip_values.append(line)
            count +=1
            if count > num_data:
                done = True
                break
        fipfile.seek(0)
    fipfile.close()
    for i in range(num_data):
        cmd_out = cmd % (i+1)
        out_lines= os.popen(cmd_out).readlines()
        for line in out_lines:
            if "X-ESPRESSO-Partition" in line:
                if line.split(':')[1].strip() == partition_name:  ## this key value belongs to the current partition
                    dbg_print (cmd_out)
                    dbg_print (line)
                    if not espresso_value_compare_line(ip_values[i],fcsmrfile.readline()):
                        retCode = RetCode.ERROR
    ## check for any remaining lines in consumer value file ( it should be at the end i.e. no more lines)
    if not fcsmrfile.readline() == '':
        retCode=RetCode.ERROR
    fcsmrfile.close()
    return retCode

def do_espresso_value_compares():
    global options
    if not (options.base_in or options.file1)  :my_error("not enough files specified")
    if not (options.base_out or options.file2) :my_error("not enough files specified")
    if not (options.range_list or options.db_partitions): my_error("db partitions not specified. You must specify either db_range or db_partitions")
    if options.range_list != False and options.db_partitions != False:
        my_error("You may specify either db_range or db_partitions, but not both")
    db_list = options.db_list.split(",")

    if options.range_list != False:
        range_list = options.range_list.split(",")
        for i,entry in enumerate(range_list):
            range_list[i] = ("0-%s" % (str(int(entry)-1)))
    if options.db_partitions != False:
        range_list = options.db_partitions.split(",")

    retcode = RetCode.OK
    for j,db_name in enumerate (db_list):
        for i in convert_range_to_list(range_list[j]):
            file_name_1 = options.file1 
            file_name_2 = options.file2 + "." + db_name + "_" + str(i)
            print "Comparing: %s and %s" % ( file_name_1, file_name_2)
            ret = espresso_value_compare_partition (db_name, str(i), file_name_1, file_name_2)
            if not (ret == RetCode.OK or ret == RetCode.ZERO_SIZE):
                print "Comparing mismatch with files: %s and %s" % ( file_name_1, file_name_2 )
                retcode = ret
                break
    return retcode

def do_concat_espresso_data_files():
    global options
    if not (options.base_in)  :my_error("not enough files specified")
    if not (options.base_in_2)  :my_error("not enough files specified")
    if not (options.base_out) :my_error("not enough files specified")
    if not options.db_list: my_error("db names not specified")
    if not options.range_list: my_error("db partitions not specified")
    base_1 = options.base_in
    base_2 = options.base_in_2
    base_out = options.base_out
    db_list = options.db_list.split(",")
    range_list = options.range_list.split(",")
    for j,db_name in enumerate (db_list):
        for i in range(int(range_list[j])):
		file1 = base_1 + "." + db_name + "_" + str(i)
                file2 = base_2 + "." + db_name + "_" + str(i)
                file_out = base_out + "-" + db_name + "_" + str(i)
                print "Concating %s and %s to %s" % (file1,file2,file_out)
                if ((not os.path.exists(file1)) or (not os.path.exists(file2))):
                    return RetCode.ERROR
                os.popen ("cat %s > %s" % (file1,file_out))
                os.popen ("cat %s >> %s" % (file2,file_out))
    return RetCode.OK

def do_espresso_cluster_compare():
    global options

    if options.cluster_merge:
        options.file1 = options.base_in
        options.file2 = options.base_out
        if os.path.exists(options.file2) and options.merge_append != True:
            os.remove(options.file2)

    if not (options.file1): my_error("not enough files specified")
    if not (options.file2): my_error("not enough files specified")
    if not (options.db_list): my_error("db names not specified")
    if not (options.range_list or options.db_partitions): my_error("db partitions not specified. You must specify either db_range or db_partitions")
    if options.range_list != False and options.db_partitions != False:
        my_error("You may specify either db_range or db_partitions, but not both")

    #convert input to comma-separated value and put it in range_list. Example: if input is 1-4,8 - then convert this as 1,2,3,4,8
    db_list = options.db_list.split(",")

    if options.range_list != False:
        range_list = options.range_list.split(",")
        for i,entry in enumerate(range_list):
            range_list[i] = ("0-%s" % (str(int(entry)-1)))
    if options.db_partitions != False:
        range_list = options.db_partitions.split(",")

    for j,db_name in enumerate (db_list):
        for i in convert_range_to_list(range_list[j]):

            file_name_merged = os.path.join(get_work_dir(), ("%s_%s_merged" % (db_name, str(i))))

            dbg_print("Cluster Compare : %s, Path Exist : %s, Not Merge Append : %s" % ((not options.cluster_merge), (os.path.exists(file_name_merged)) , (options.merge_append != True)))

            if ((not options.cluster_merge) and (os.path.exists(file_name_merged)) and (options.merge_append != True)):
		os.remove(file_name_merged)
  
            #find which relays hosts this partition
            relay_list = get_relays(options.client_host, options.client_port, db_name, i, options.master, options.slave)
            for relay_name in relay_list:
                curr_relay_host = relay_name.split(':')[0].strip()
                curr_relay_port = relay_name.split(':')[1].strip()
                base_1 = ("%s_%s_%s" % (options.file1, curr_relay_host, curr_relay_port))
                base_2 = options.file2
                file_name_1 = ("%s.%s_%s" % (base_1, db_name, str(i)))
                if options.cluster_merge:
                    file_name_2 = options.file2
                    print "Merging: %s into %s" % (file_name_1, file_name_2)
                    if not os.path.exists(file_name_1):
                        return RetCode.ERROR
                    os.popen("cat %s >> %s" % (file_name_1, file_name_2))
                else:
                    file_name_2 = ("%s.%s_%s" % (base_2, db_name, str(i)))

		    if not options.compare_without_merge:
		    	os.popen("cat %s >> %s" % (file_name_1, file_name_merged))

                    ret = do_compare (file_name_merged, file_name_2, None, options.file1_offset, options.file2_offset)
                    if not (ret == RetCode.OK or ret == RetCode.ZERO_SIZE):
                        print "Mismatch between  files: %s and %s" % ( file_name_merged, file_name_2 )
                        return ret

    if options.cluster_merge and options.expected_event_count != False:
	fullFileName = os.path.join(get_view_root(), file_name_2)
        catCmd = ("cat %s | wc -l" % (fullFileName))
        totalEvents = int(sys_pipe_call(catCmd))
	expectedEvents = int(options.expected_event_count)
	if totalEvents != expectedEvents:
            print ("Number of events does not match. Expected: %s. Actual: %s" % (str(expectedEvents), str(totalEvents)))
            return RetCode.ERROR
    return RetCode.OK

def do_parse_espresso_files():
    global options
    if not (options.base_in or options.file1)  :my_error("not enough files specified")
    if not (options.base_out or options.file2) :my_error("not enough files specified")
    if not options.db_list: my_error("db names not specified")
    if not options.range_list: my_error("db partitions not specified")
    base_1 = ""
    base_2 = ""
    offset1 = options.file1_offset
    offset2 = options.file2_offset
    if options.merge:
        base_1 = options.base_in
        base_2 = options.base_out
        if os.path.exists(base_2):
            os.remove(base_2)
    else:
        base_1 = options.file1
        base_2 = options.file2
    
    if ( not offset1):
    	offset1 = 0
    if ( not offset2):
    	offset2 = 0

    db_list = options.db_list.split(",")

    range_list = options.range_list.split(",")

    retcode = RetCode.OK

    for j,db_name in enumerate (db_list):
        for i in range(int(range_list[j])):
            if options.merge:
                file_name = base_1 + "." + db_name + "_" + str(i)
                print "Merging: %s" % file_name
                if not os.path.exists(file_name):
                    return RetCode.ERROR
                os.popen ("cat %s >> %s" % (file_name,base_2))
            else:
                file_name_1 = base_1 + "." + db_name + "_" + str(i)
                file_name_2 = base_2 + "." + db_name + "_" + str(i)
                print "Comparing: %s and %s" % ( file_name_1, file_name_2)
                ret = do_compare (file_name_1, file_name_2, None, options.file1_offset, options.file2_offset)
                if not (ret == RetCode.OK or ret == RetCode.ZERO_SIZE):
                    print "Comparing mismatch with files: %s and %s" % ( file_name_1, file_name_2 )
                    retcode = ret
                    break
    if options.merge:
        print "Done! Filesize:%s" % os.stat(base_2)[6]
        if (os.stat(base_2)[6] == 0):
            print ("Error: 0 file size. No data")
            retcode = RetCode.ERROR
    if options.merge and options.expected_event_count != False:
	fullFileName = os.path.join(get_view_root(), base_2)
        catCmd = ("cat %s | wc -l" % (fullFileName))
        totalEvents = int(sys_pipe_call(catCmd))
	expectedEvents = int(options.expected_event_count)
	if totalEvents != expectedEvents:
            print ("Number of events does not match. Expected: %s. Actual: %s" % (str(expectedEvents), str(totalEvents)))
            return RetCode.ERROR
    return retcode

def main(argv):
    global options, outf, args, input_files

    parser = OptionParser(usage="usage: %prog [options] file1 [file2]")
    parser.add_option("-c", "--compress", action="store_true", dest="compress", default = False,
                       help="Compress the result and then compare. If only one file is given, just give the compressed output. Default to %default")
    parser.add_option("-s", "--save_copy", action="store_true", dest="save_copy", default = False,
                       help="Save copy of the files before diffing. Default to %default")

    parser.add_option("--espresso_concat", action="store_true", dest="espresso_concat", default = False,
                       help="concat 2 list of data files")
    parser.add_option("-m", "--merge", action="store_true", dest="merge", default = False,
                       help="merge a set of dump files")
    parser.add_option("-e", "--espresso_compare", action="store_true", dest="espresso_compare", default = False,
                       help="espresso compare")
    parser.add_option("-v", "--espresso_value_compare", action="store_true", dest="espresso_value_compare", default = False,
                       help="espresso value compare")
    parser.add_option("--espresso_cluster_compare", action="store_true", dest="espresso_cluster_compare", default = False,
                       help="espresso cluster-based compare")
    parser.add_option("--cluster_merge", action="store_true", dest="cluster_merge", default = False,
                       help="merge a set of dump files (cluster-aware version)")
    parser.add_option("--out", action="store", dest="base_out", default = False,
                       help="Espresso DB output consolidated file")
    parser.add_option("--in", action="store", dest="base_in", default = False,
                       help="Espresso DB input base file")
    parser.add_option("--in_2", action="store", dest="base_in_2", default = False,
                       help="Espresso DB input base file2")
    parser.add_option("--file1", action="store", dest="file1", default = False,
                       help="Espresso DB compare file 1")
    parser.add_option("--file2", action="store", dest="file2", default = False,
                       help="Espresso DB compare file 2")
    parser.add_option("--file1_offset", action="store", dest="file1_offset", default=None,
                       help="Espresso DB compare file 1 line offset ")
    parser.add_option("--file2_offset", action="store", dest="file2_offset", default=None,
                       help="Espresso DB compare file 2 line offset ")
    parser.add_option("--master", action="store", dest="master", default = True,
                       help="Whether to compare master logs")
    parser.add_option("--slave", action="store", dest="slave", default = False,
                       help="Whether to compare slave logs")
    parser.add_option("--db_list", action="store", dest="db_list", default = False,
                       help="Espresso DB base file")
    parser.add_option("--db_range",  action="store", dest="range_list", default = False,
                       help="Espresso DB file range. For example: 3 will compare partitions 0,1,2. You may either specify db_range or db_partitions, but not both.")
    parser.add_option("--db_partitions", action="store", dest="db_partitions", default = False,
                       help="Espresso DB partition range. Separate partition list for same db using semi-colon (;). Separate partition list for different db using comma (,). For example: 0-4;8,5-7 will compare partitions 0,1,2,3,4,8 for first db, and partitions 5,6,7 for second db. You may either specify db_range or db_partitions, but not both.")
    parser.add_option("--espresso_host", action="store", dest="espresso_host", default = False,
                       help="Hostname of espresso storage node. (Could be router also)")
    parser.add_option("--espresso_port", action="store", dest="espresso_port", default = False,
                       help="Port name of espresso storage node. (Could be router also)")
    parser.add_option("--espresso_key_range", action="store", dest="espresso_key_range", default = False,
                       help="range of keys to verify")
    parser.add_option("--client_host", action="store", dest="client_host", default = "localhost",
                       help="Hostname where databus client is running")
    parser.add_option("--client_port", action="store", dest="client_port", default = "10564",
                       help="port where databus client is running")
    parser.add_option("--expected_event_count", action="store", dest="expected_event_count", default=False,
                       help="When merging files, this option will verify that the number of events in merged file equals the expected amount")
    parser.add_option("--append", action="store", dest="merge_append", default=True,
                       help="When merging files, this option will append to the end of existing file")
    parser.add_option("--compare_without_merge", action="store", dest="compare_without_merge", default=False,
                       help="When comparing files, this option specifically disables appending to  end of merge file")

    parser.add_option("-l", "--silent", action="store_true", dest="silent", default = False,
                       help="Silent mode. Do not print the diff. Just give return code. Default to %default")
    parser.add_option("-o", "--output", action="store", dest="output", default=None,
                       help="Output file name. Default to stdout")
    parser.add_option("","--from_scn", action="store", type="long", dest="from_scn", default=None,
                       help="The scn to start compare. Default to the first scn in the files")
    parser.add_option("","--end_scn", action="store", type="long", dest="end_scn", default=None,
                       help="The scn to start compare. Default to the last scn in the files")
    parser.add_option("","--keyMin", action="store", type="long", dest="keyMin", default=None,
                       help="The min key of the consumer range to compare on. Need to set both keyMin and keyMax to take effect.")
    parser.add_option("","--keyMax", action="store", type="long", dest="keyMax", default=None,
                       help="The max key of the consumer range to compare on. Need to set both keyMin and keyMax to take effect.")
    parser.add_option("--enable_consumer_key_range", action="store_true", dest="enable_consumer_key_range", default = False,
                       help="Send if enable the key range for the second input (consumer)")
    parser.add_option("--server_side_filter", action="store", dest="server_side_filter", default = None,
                       help="Semicolon delimited server side filtering function similar to the property file, e.g., type=range;range.size=100;range.partitions=[1,3-4];key=id")
    parser.add_option("-i", "--ignored_columns", action="store", dest="ignored_columns", default=None,
                       help="Comma separated list of columsn to ignore during the comparison")
    parser.add_option("--ignore_json_error", action="store_true", dest="ignore_json_error", default = False,
                       help="If set ignore the json error")
    parser.add_option("--compress_key_only", action="store_true", dest="compress_key_only", default = False,
                       help="output key only during compress")
    parser.add_option("--no_json_conversion", action="store_true", dest="no_json_conversion", default = False,
                       help="do not convert to json, do straight compare")
    parser.add_option("-n", "--testname", action="store", dest="testname", default=None, help="A test name identifier")

    db_compare_group = OptionGroup(parser, "DB compare options", "")
    db_compare_group.add_option("--db_src_ids", action="store", dest="db_src_ids", default=None,
                       help="Source IDs. Comma separated list. For dbgen, it is the datasource name in the json, e.g., member2. Default to %default")
    db_compare_group.add_option("--db_config_file", action="store", dest="db_config_file", default="integration-test/config/sources-member2.json",
                       help="The db config file to use. Default to %default")
    db_compare_group.add_option("--sort_key", action="store_true", dest="sort_key", default = False,
                       help="sort the keys")
    db_compare_group.add_option("--fk_src_order", action="store", dest="fk_src_order", default=None,
                       help="src ids for pk->fk constraint., e.g. 40,41 means 40 is a parent of 41")
    db_compare_group.add_option("--fk_src_order_only", action="store_true", dest="fk_src_order_only", default=False,
                       help="Check sequence only. Use regular expression to parse lines")
    db_compare_group.add_option("--check_event_count", action="store_true", dest="check_event_count", default=False,
                       help="Check the event count should match the scn for specially generated workload [Default to %default]")
    db_compare_group.add_option("--check_key_sum", action="store_true", dest="check_key_sum", default=False,
                       help="Check key sum in windows match the scn for specially generated workload [Default to %default]")
    db_compare_group.add_option("--db_xml_dump_file", action="store", dest="db_xml_dump_file", default=None,
                       help="if given, use the xml instead of query the db. Mostly for testing")
    parser.add_option_group(db_compare_group)

    remote_group = OptionGroup(parser, "Remote options", "")
    # maybe we can figure this out from the file name
    remote_group.add_option("", "--services", action="store", dest="services", default=None,
                       help="Comma separated list. Used in remote invokation to figure out which machine to go")
    remote_group.add_option("", "--component1", action="store", dest="component1", default = None, choices=components,
                       help="The component of the first file. Default will be the relay in remote config file")
    remote_group.add_option("", "--component1_id", action="store", dest="component1_id", default = None,
                       help="The compnent id (1,2..) of the first file")
    remote_group.add_option("", "--component2", action="store", dest="component2", default = None, choices=components,
                       help="The component of the second file. Default will be consumer in remote config file")
    remote_group.add_option("", "--component2_id", action="store", dest="component2_id", default = None,
                       help="The compnent id (1,2..) of the second file")
    parser.add_option_group(remote_group)

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

    if options.cluster_merge and options.espresso_cluster_compare:
        my_error("compare and merge cannot be done in one step. Use different steps")

    setup_work_dir()

    if options.espresso_value_compare:
       ret = do_espresso_value_compares()
       sys.exit(ret)
    if options.espresso_concat:
	ret = do_concat_espresso_data_files()
	sys.exit(ret)
    if options.merge or options.espresso_compare:
       ret = do_parse_espresso_files() 
       sys.exit(ret)
    if options.cluster_merge or options.espresso_cluster_compare:
       ret = do_espresso_cluster_compare()
       sys.exit(ret)
    arg_error = False
    if len(args)>2: 
      print "Error: More than 2 files are given"
      arg_error = True
    if options.fk_src_order and options.db_src_ids:
      print "Error: both fk_src_ids and db_src_ids is given. Db compare more cannot do fk src check."
      arg_error = True
    if len(args)==0 and not options.db_src_ids:  # not compare with the db
      print "Warning: No input files are given"
      arg_error = True
    if arg_error: 
      parser.print_help()
      parser.exit()
    if (not options.testname):
      options.testname = "TEST_NAME" in os.environ and os.environ["TEST_NAME"] or "default"
    os.environ["TEST_NAME"]= options.testname;
    if (not "WORK_SUB_DIR" in os.environ):
      os.environ["WORK_SUB_DIR"] = "log"
    if (not "LOG_SUB_DIR" in os.environ):
      os.environ["LOG_SUB_DIR"] = "log"

    setup_env()

    input_files = args
    if not remote_run: input_files = check_exists(input_files)
    if options.save_copy or remote_run:  save_copy(input_files)
    filterFunc = None
    if (options.keyMin and options.keyMax) or options.server_side_filter:
      filterFunc = KeyRangeFilter(options.keyMin, options.keyMax, options.server_side_filter)
  
    if options.db_src_ids:
      db_generate_json()
      input_files.insert(0, db_json_dump_file)
      if len(input_files)==2: print "Comparing %s with db json dump" % input_files[1] 

    if options.fk_src_order:     # init the fk src state
      global fk_src_id_order, fk_src_id_state, fk_src_id_order_index
      fk_src_id_order = [int(x) for x in options.fk_src_order.split(",")]
      for x in fk_src_id_order: fk_src_id_order_index[x] = fk_src_id_order.index(x)
      dbg_print("fk_src_id_order = %s" % fk_src_id_order)
      dbg_print("fk_src_id_order_index = %s" % fk_src_id_order_index)
      for i in range(len(input_files)): 
        fk_src_id_state.append({})
        for src_id in fk_src_id_order: fk_src_id_state[i][src_id] = {"maxSeq":-1}
      
    if options.output: outf=open(options.output,"w")
    ret = RetCode.OK
    if len(input_files)==1: 
      if options.db_src_ids:
        outf.writelines(open(input_files[0]).readlines())
      elif options.compress: 
        compressed_lines = compress_input(open(input_files[0]))
        #import cProfile
        #cProfile.run(compress_input(open(input_files[0])),'profile.txt')
        #cProfile.runctx('compress_input_1("%s")' % input_files[0],globals(), locals(),'/Users/dzhang/project/databus2/integration-test/var/work/events/liar_event_35_1/profile.txt')
        #cProfile.runctx('compress_input_1("%s")' % input_files[0],globals(), locals())
        if not options.fk_src_order: # only print out when not checkin source order
          for line in compressed_lines: outf.write("%s\n" % json.dumps(line))
      elif options.fk_src_order: # just check the sequence
        check_sequence(open(input_files[0]))
      else: print "Do nothing as only one file %s is given and compress option is not specified" % input_files[0]
    else: 
        if os.stat(input_files[0])[6] == 0: 
            print "Empty File: %s" % input_files[0]
            ret = RetCode.ERROR
        if os.stat(input_files[1])[6] == 0: 
            print "Empty File: %s" % input_files[1]
            ret = RetCode.ERROR
        ret = do_compare(input_files[0], input_files[1], filterFunc)

    if ret == RetCode.OK and options.fk_src_order: # check sequence
      for i in range(len(input_files)):
        for src_id in fk_src_id_order:
          if "Error" in fk_src_id_state[i][src_id]: 
            print "Sequence error in %s: %s" % (input_files[i], fk_src_id_state[i][src_id]["Error"])
            ret = RetCode.ERROR

    if ret == RetCode.OK: print "ok"
    outf.close()
    sys.exit(ret)
     
if __name__ == "__main__":
    main(sys.argv[1:])
