#!/usr/bin/env python

'''
  Generate event
  Can also load from a file
  Will handle remote file load in the future

'''
__author__ = "David Zhang"
__version__ = "$Revision: 0.1 $"
__date__ = "$Date: 2010/11/16 $"

import sys, os
import pdb
import time, copy, re
from optparse import OptionParser, OptionGroup
import logging
#import subprocess
from utility import *
        
# Global varaibles
options=None
server_host="localhost"
server_port="9000"
''' curl -v "localhost:8080/loadDataEvents?file=srcevents-json.out&startWindow=true" '''

load_cmd_template=None
start_cmd_template=None
db_start_cmd_template=None
single_cmd=None
db_single_cmd_template=None

def setup_cmd_template():
    global load_cmd_template,start_cmd_template,single_cmd_template, db_start_cmd_template, db_single_cmd_template
    load_cmd_template="http://%s:%s/loadDataEvents?%%s" % (server_host, server_port)
    start_cmd_template="http://%s:%s/genDataEvents/start?src_ids=%%s&fromScn=%%s&eventsPerSec=%%s&duration=%%s" % (server_host, server_port)
    single_cmd_template="http://%s:%s/genDataEvents/%%s" % (server_host, server_port) # cmd could be start, stop, resume
    #db_start_cmd_template="http://%s:%s/controlSources/start?sources=%%s&scn=%%s" % (server_host, server_port)
    db_start_cmd_template="http://%s:%s/controlSources/start?sources=%%s" % (server_host, server_port)
    db_single_cmd_template="http://%s:%s/controlSources/%%s" % (server_host, server_port) # cmd could be shutdown, pause, unpause, status

def setup_host_port():
    global server_host, server_port
    if options.server_host: server_host = options.server_host
    if options.server_port: server_port = options.server_port

def setup_remote_run():
    for section in remote_run_config:
      # TODO: handle id later
      if re.search("_relay", section): 
        cmd_section=section
        break
    if not cmd_section: my_error("No section for relay")
    # TODO, handle port later
    global server_host, server_port
    server_host= remote_run_config[cmd_section]["host"]
    server_port= remote_run_config[cmd_section]["port"]
    dbg_print("server_host = %s, server_port = %s" % (server_host, server_port))

def setup_env():
    dbg_print("remote_run_config = %s" % remote_run_config)
    if remote_run: setup_remote_run()
    setup_host_port()
    setup_cmd_template()

def load_file_send(file_name):
    dbg_print("file_name = %s" % file_name)
    if options.debug:
      for l in open(file_name): dbg_print("l = %s" % l[:100])
    paras=[("file",file_name)]
    if options.start_window: paras.append(("startWindow","true"))
    url_str=load_cmd_template % urllib.urlencode(paras)
    send_url(url_str)

endw_line_dict=json.loads('''{"key":0,"sequence":9449859746,"logicalPartitionId":0,"physicalPartitionId":0,"timestampInNanos":22281768406372543,"srcId":-2,"schemaId":"AAAAAAAAAAAAAAAAAAAAAA==","valueEnc":"JSON","endOfPeriod":true,"value":""}''');
def load_file_send_end_of_window(fout, seq):
    endw_line_dict['sequence']= seq
    fout.write("%s\n" % json.dumps(endw_line_dict))
 
def load_file():
    url_template = "http://%s:%s/containerStats/inbound/events/total"    
    url_str = url_template % (server_host, server_port)
    field = "numDataEvents"

    if options.file!="stdin":
      input_file = file_exists(options.file) 
      if not input_file: my_error("Input file %s does not exist" % options.file)
      inputf = open(input_file)
    else:
      inputf = sys.stdin

    # break down the input into batches
    fd, tmp_file = tempfile.mkstemp()
    dbg_print("tmp_file = %s" % tmp_file)
    fout = open(tmp_file, 'w')
    line_count = 0
    total_event = 0
    previous_seq=None
    seq_re = re.compile('''"sequence":([0-9]+),''')
    for line in inputf: 
       sequence = long(seq_re.search(line).groups()[0])
       new_window = (previous_seq !=None and sequence != previous_seq)
       if options.file_total_event and total_event >= options.file_total_event: break
       if (options.file_batch_size and line_count >= options.file_batch_size) or new_window:
         increased_by = line_count     # status check
         line_count = 0
         if new_window: load_file_send_end_of_window(fout, previous_seq)
         fout.close()
         #load_file_send(tmp_file)
         dbg_print("pre_seq = %s, seq = %s, new_window = %s " % (previous_seq, sequence, new_window))
         #wait_num_increased(url_str, field, increased_by)
         wait_num_increased('load_file_send("%s")' % tmp_file, url_str, field, increased_by)
         #pdb.set_trace()
         fout = open(tmp_file, 'w')
       fout.write("%s" % line)
       line_count +=1
       total_event += 1
       #dbg_print("break into batches. line_count = %s" % line_count)
       previous_seq = sequence
    dbg_print("out of for loop: previous_seq = %s" % previous_seq)
    if previous_seq: load_file_send_end_of_window(fout, previous_seq) # this will close the file
    fout.close()
    if line_count!=0:
      load_file_send(tmp_file)
    # send the end of window
    #dbg_print("previous_line = %s" % previous_line)

def call_espresso_gen():
    espresso_put_template = ""
    delay = 0.0
    if options.event_per_sec:
        delay = 1.0 / options.event_per_sec
    print ("delay=%s"%delay)
    start_time = time.time()
    if options.two_level_keys:
        espresso_put_template = "curl -s -D - -H \"Content-Type:application/json\" -X PUT --data-binary '%%s' http://%s:%s/%%s/%%s/1/%%s" % (options.espresso_host,options.espresso_port)
    else:
        espresso_put_template = "curl -D - -H \"Content-Type:application/json\" -X PUT --data-binary '%%s' http://%s:%s/%%s/%%s/%%s" % (options.espresso_host, options.espresso_port)

    count = 1
    done=False 
    fout = open ( options.espresso_data_file, 'r')
    db_name = options.db_name 
    db_table = options.db_table 
    if not options.num_events:
        for line in fout:
            event_start_time = time.time()
            cmd = espresso_put_template % (line .rstrip('\n'), db_name, db_table, count)
            dbg_print(cmd)
            sys_pipe_call(cmd)
            event_delta = time.time() - event_start_time 
            if (delay != 0):
                time_sleep = delay - event_delta
                if ( time_sleep > 0 ) :
                    time.sleep(time_sleep)
            count +=1
    else:
        curr = 1;
        while not done:
            for line in fout:
		if ( options.event_offset and curr <= options.event_offset):
			dbg_print(("Skipping line %s",line))
			curr+=1
			continue
                event_start_time = time.time()
                cmd = espresso_put_template % (line .rstrip('\n'), db_name, db_table, curr)
                dbg_print(cmd)
                sys_pipe_call(cmd)
                count +=1
		curr += 1
                if count > options.num_events:
                    done = True
                    break
                event_delta = time.time() - event_start_time 
                #dbg_print ("event_delta=%s"%event_delta)
                if (delay != 0):
                    time_sleep = delay - event_delta
                    #dbg_print ("time_sleep=%s"%time_sleep)
                    if ( time_sleep > 0 ) :
                        time.sleep(time_sleep)
            fout.seek(0)
        fout.close()
    elapsed_time = time.time() - start_time
    print("Data Written: %s, Elapsed Time: %s seconds" % (count,elapsed_time))
def call_db_generator():
    url_str=db_start_cmd_template % (options.src_ids)
    if options.from_scn: 
      url_str+= "&scn=%s" % (options.from_scn)
    if options.stop_gen: url_str=db_single_cmd_template % "shutdown"
    if options.suspend_gen: url_str=db_single_cmd_template % "pause"
    if options.resume_gen: url_str=db_single_cmd_template % "unpause"
    if options.status: url_str=db_single_cmd_template % "status"
    out = send_url(url_str)
    if not options.status:
      time.sleep(1)  # wait for the option to complete
    print "url_str= %s" % url_str
    print "Output= %s" % out
    if options.from_scn: 
      wait_status_change("running", options.src_ids)

def call_random_generator():
    url_str=start_cmd_template % (options.src_ids, options.from_scn, options.event_per_sec, options.duration)
    if options.stop_gen: url_str=single_cmd_template % "stop"
    if options.suspend_gen: url_str=single_cmd_template % "suspend"
    if options.resume_gen: url_str=single_cmd_template % "resume?"
    if options.status: url_str=single_cmd_template % "check"
    if not options.stop_gen and not options.suspend_gen:
      if options.num_events: url_str += "&eventsToGenerate=%s" % options.num_events
      if options.percent_buffer: url_str += "&percentBuffer=%s" % options.percent_buffer
      if options.keyMin: url_str += "&keyMin=%s" % options.keyMin
      if options.keyMax and options.keyMax != 215670000000: url_str += "&keyMax=%s" % options.keyMax
    out = send_url(url_str)
    print "url_str= % s" % url_str
    print "Output= % s" % out

# db related
db_conn_user_id = None
db_conn_user_passwd = None
db_sid=None
db_host=None
db_src_info={}
db_src_ids=None  # list of int of source ids

def db_get_conn_info():
    global db_conn_user_id, db_sid, db_host, db_conn_user_passwd, db_src_info, db_src_ids
    (db_conn_user_id, db_sid, db_host, db_conn_user_passwd, db_src_info, db_src_ids) = parse_db_conf_file(options.db_config_file, "all")
    (db_host, db_port) = db_config_detect_host(db_host, detect_oracle=True, db_user=db_conn_user_id, passwd=db_conn_user_passwd, db_sid=db_sid)

def call_db_testdata_reload():
    ''' reload the test db '''
    db_get_conn_info()
    for src_id in db_src_info:
      db_user_id = db_src_info[src_id]["db_user_id"]
      qry = "exec %s.Test.unloadCoreTestData; \n commit; \n exec %s.Test.loadCoreTestData; \n commit" % (db_user_id, db_user_id)
      ret = exec_sql(qry, db_conn_user_id, db_conn_user_passwd, db_sid, db_host)

def call_db_testdata_truncate():
    ''' reload the test db '''
    db_get_conn_info()
    for src_id in db_src_info:
      db_user_id = db_src_info[src_id]["db_user_id"]
      db_src_uri = db_src_info[src_id]["uri"]
      #qry = "truncate table %s; \n truncate table %s.sy$txlog; \n commit" % (db_src_uri, db_user_id)
      # truncate sys$txlog will hit DDS-479 
      qry = "delete from %s.sy$txlog; \n commit" % (db_user_id)
      ret = exec_sql(qry, db_conn_user_id, db_conn_user_passwd, db_sid, db_host)
      if re.search("_[0-9]$",db_src_uri): db_src_uri = "_".join(db_src_uri.split("_")[:-1])
      qry = "truncate table %s; \n  commit" % (db_src_uri)
      ret = exec_sql(qry, db_conn_user_id, db_conn_user_passwd, db_sid, db_host)

def call_db_load_large_event():
    ''' load the db with large event '''
    db_get_conn_info()
    for src_id in db_src_info:
      db_user_id = db_src_info[src_id]["db_user_id"]
      # only do bizfollow for now
      if src_id == 101:     # bizfollow
        qry = '''
declare
  l_clob clob;
  tmp_clob clob;
begin
  DBMS_LOB.createtemporary (lob_loc => tmp_clob, cache => TRUE);
  tmp_clob := RPAD('A',32000,'A');
  DBMS_LOB.createtemporary (lob_loc => l_clob, cache => TRUE);
  for i in 1..500
  loop
    DBMS_LOB.append (l_clob, tmp_clob);
  end loop;
  update bizfollow.biz_follow set NOTIFY_OPTIONS=l_clob where id=1;
  commit;
end;
/
'''
        ret = exec_sql(qry, db_conn_user_id, db_conn_user_passwd, db_sid, db_host)

def call_db_testdata_insert():
    ''' insert some data into the db '''
    db_get_conn_info()
    insert_qry = None
    if options.src_ids == "liar":
      get_max_id_qry = '''
  select decode(max(event_id),null,0,max(event_id)) into max_id from liar.liar_job_relay;
  select decode(max(event_id),null,0,max(event_id)) into max_id_1 from liar.liar_member_relay;
'''
      insert_qry='''
  insert into liar.liar_job_relay (event_id) values (cnt + max_id);
  commit;
  insert into liar.liar_member_relay (event_id) values (cnt + max_id_1);
  commit;
'''
    elif options.src_ids == "bizfollow":
      get_max_id_qry = '''select decode(max(id),null,0,max(id)), decode(max(member_id),null,0,max(member_id)) into max_id, max_id_1 from bizfollow.biz_follow;'''
      insert_qry='''
    insert into bizfollow.biz_follow (id, member_id, company_id, notify_options, notify_nus, source, status, created_on, last_modified) values (cnt + max_id, cnt + max_id_1, 1001, '<BizFollowNotifyOptions><peopleUpdates>1</peopleUpdates><jobPostings>1</jobPostings><profileUpdates>1</profileUpdates><blogPosts>1</blogPosts></BizFollowNotifyOptions>', 1, 1, 'A', now_v, now_v);
    commit;
'''
    if insert_qry:
      num_events = options.num_events and options.num_events or 10
      interval_delay = options.insert_interval_delay and options.insert_interval_delay or 1
      interval_delay_hundreds_secs = max(interval_delay/10,1)
      interval_delay_count = 10/interval_delay
      qry = '''
declare
  now_v timestamp;
  cur_id number;
  delay_count number;
  max_id number;
  max_id_1 number;
begin
  now_v := sysdate;
  %s
  delay_count := 0;
  for cnt in 1..%s
  loop
    %s
    delay_count := delay_count + 1;
    if delay_count >= %s then 
      dbms_lock.sleep(0.01 * %s);
      delay_count := 0;
    end if;
  end loop;
end;
/
''' % (get_max_id_qry, num_events, insert_qry, interval_delay_count, interval_delay_hundreds_secs)
      ret = exec_sql(qry, db_conn_user_id, db_conn_user_passwd, db_sid, db_host)

def wait_field_value(url_str, field, target, is_num=False):
    ''' return to true if the field becomes target '''
    print "WAIT_URL:", url_str
    out = send_url(url_str)
    if re.search("Exception", out): my_error("Get exception checking status\n %s" % out)
    out_status  = quote_json(out.split("\n")[1])
    dbg_print("out_status = %s " % out_status)
    dbg_print("field = %s, target = %s " % (field, target))
    out_json = json.loads(out_status)
    #pdb.set_trace()
    if field in out_json: 
      if target=="get_field_value": return out_json[field]
      else: 
        dbg_print("current target = %s" % out_json[field])
        if not is_num: return out_json[field] == target
        else: return int(out_json[field]) == int(target)
    else: my_error("Field %s not in output of status call \n %s \n out_json=%s" % (field, out, out_json))

def wait_for_condition(cond, timeout=60, sleep_interval = 0.1):
    ''' wait for a certain cond. cond could be a function. 
       This cannot be in utility. Because it needs to see the cond function '''
    dbg_print("cond = %s" % cond)
    sleep_cnt = 0
    ret = RetCode.TIMEOUT
    while (sleep_cnt * sleep_interval < timeout):
      if eval(cond): 
        ret = RetCode.OK
        break
      time.sleep(sleep_interval)
      sleep_cnt += 1
    return ret

def wait_num_increased(operation, url_str, field, increased_by):
    old_num = wait_field_value(url_str, field, "get_field_value")
    new_num = old_num + increased_by
    eval(operation)   # do the operation
    ret = wait_for_condition('wait_field_value("%s", "%s", "%s", True)' % (url_str, field, new_num), options.timeout, 0.01)
    if ret == RetCode.TIMEOUT: print "Timed out waiting for number %s to increase by %s" % (old_num, increased_by)
    return ret 
     
def wait_status_change(status, src_ids):
    if options.db_gen: 
      url_str=db_single_cmd_template % "status"
      field = "status"
      value = status=="suspend" and "paused" or "running"
    else: 
      url_str=single_cmd_template % "check"
      field = "genDataEventsRunning"
      value = status=="suspend" and "false" or "true"
    # status of a specific physical sourcs
    if src_ids != None:
        url_str += "?sources=" + src_ids
    print "WaitStatusChange:" + url_str
    ret = wait_for_condition('wait_field_value("%s", "%s", "%s")' % (url_str, field, value), options.timeout, 1)
    if ret == RetCode.TIMEOUT: print "Timed out waiting for status change to %s" % status
    return ret
 
def main(argv):
    global options

    parser = OptionParser(usage="usage: %prog [options]")
#    parser.add_option("-c", "--component", action="store", dest="component", default=None, choices=components,
#                       help="default to search the relay. %s" % componnents)
    parser.add_option("-s", "--src_ids", action="store", dest="src_ids", default="1",
                       help="Source IDs. Comma separated list. For dbgen, it is the datasource name in the json, e.g., member2. Default to %default")
    parser.add_option("-b","--from_scn", action="store", type="long", dest="from_scn", default=1,
                       help="The start scn. Default to %default")
    parser.add_option("-e","--event_per_sec", action="store", type="int", dest="event_per_sec", default=10,
                       help="Default to %default")
    parser.add_option("-t","--duration", action="store", type="long", dest="duration", default=3600000000,
                       help="Duration in ms. Default to %default")
    parser.add_option( "--keyMin", action="store", type="long", dest="keyMin", default = 0,
                       help="minimum key for range")
    parser.add_option( "--keyMax", action="store", type="long", dest="keyMax", default = 215670000000,
                       help="maximum key for range")
    parser.add_option( "--stop_gen", action="store_true", dest="stop_gen", default = False,
                       help="stop generation")
    parser.add_option( "--suspend_gen", action="store_true", dest="suspend_gen", default = False,
                       help="suspend generation")
    parser.add_option( "--resume_gen", action="store_true", dest="resume_gen", default = False,
                       help="resume generation")
    parser.add_option("--num_events", action="store", type="long", dest="num_events", default=None,
                       help="Number of events to generate before suspend. Default to %default")
    parser.add_option("--event_offset", action="store", type="long", dest="event_offset", default=None,
                       help="Offset of the data file from which to start generating. Default to %default")
    parser.add_option("--percent_buffer", action="store", type="int", dest="percent_buffer", default=None,
                       help="Percentage of event buffer to fill before suspend. Default to %default")
    parser.add_option( "--status", action="store_true", dest="status", default = False,
                       help="status of generator")
    parser.add_option( "--wait_until_suspend", action="store_true", dest="wait_until_suspend", default = False,
                       help="wait until the status becomes suspend")
    parser.add_option("--timeout", action="store", type="int", dest="timeout", default=300,
                       help="wait time out in secs. Default to %default")
    parser.add_option("--server_host", action="store", dest="server_host", default=None,
                       help="The server host to send event generation request")
    parser.add_option("--server_port", action="store", dest="server_port", default=None,
                       help="The server port to send event generation request")
 
    load_group = OptionGroup(parser, "Load file options", "")
    parser.add_option("-f", "--file", action="store", dest="file", default=None,
                       help="File with json format event to load. Could be absolute or from basedir.")
    load_group.add_option( "--start_window", action="store_true", dest="start_window", default = True,
                       help="If start a new window. Default to %default")
    load_group.add_option( "--file_batch_size", action="store", type="long", dest="file_batch_size", default = 0,
                       help="Break a file w/o EndofWindow into batches. Default to %default, no batch")
    load_group.add_option( "--file_total_event", action="store", type="long", dest="file_total_event", default = 0,
                       help="Total num of event to load from file. Default to %default, all the event")
    parser.add_option_group(load_group)

    db_gen_group = OptionGroup(parser, "DB generator options", "")
    db_gen_group.add_option("", "--db_gen", action="store_true", dest="db_gen", default = False,
                       help="start and stop are against db generator")
    db_gen_group.add_option("", "--db_testdata_reload", action="store_true", dest="db_testdata_reload", default = False,
                       help="reload the test data")
    db_gen_group.add_option("", "--db_testdata_truncate", action="store_true", dest="db_testdata_truncate", default = False,
                       help="truncate the source tables")
    db_gen_group.add_option("", "--db_testdata_insert", action="store_true", dest="db_testdata_insert", default = False,
                       help="insert some test data")
    db_gen_group.add_option("", "--insert_interval_delay", type="int", action="store", dest="insert_interval_delay", default = False,
                       help="delay between inserts in milliseconds")
    db_gen_group.add_option("", "--db_load_large_event", action="store_true", dest="db_load_large_event", default = False,
                       help="update large data")
    db_gen_group.add_option("--db_config_file", action="store", dest="db_config_file", default="config/sources-member2.json",
                       help="The db config file to use. Default to %default")
    db_gen_group.add_option("--espresso_data_file", action="store", dest="espresso_data_file", default=False,
                       help="Data file to load espresso data from")
    db_gen_group.add_option("--espresso_db_name", action="store", dest="db_name", default=False,
                       help="Data file to load espresso data from")
    db_gen_group.add_option("--espresso_table_name", action="store", dest="db_table", default=False,
                       help="Data file to load espresso data from")
    db_gen_group.add_option("--espresso_gen", action="store_true", dest="espresso_gen", default=False,
                       help="Load Espresso storage node with data from espresso_data_file file.")
    db_gen_group.add_option("--two_level_keys", action="store_true", dest="two_level_keys", default=False,
                       help="espresso schema has two level of primary keys.")
    db_gen_group.add_option("", "--espresso_host", action="store", dest="espresso_host", default = "localhost",
                       help="The port to connect to espresso. [default: %default]")
    db_gen_group.add_option("", "--espresso_port", type="int", action="store", dest="espresso_port", default = 12918,
                       help="The port to connect to espresso. [default: %default]")
    parser.add_option_group(db_gen_group)

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

    setup_env()
    ret = RetCode.OK
    do_default = True
    if options.db_testdata_reload:
      if not options.db_config_file: my_error("Please specify the db config file when reloading testdata")
      call_db_testdata_reload()
      do_default = False
    if options.db_testdata_truncate:
      if not options.db_config_file: my_error("Please specify the db config file when reloading testdata")
      call_db_testdata_truncate()
      do_default = False
    if options.file: 
      load_file()
      do_default = False
    if options.db_gen:
      call_db_generator() 
      do_default = False
    if options.db_load_large_event:
      if not options.db_config_file: my_error("Please specify the db config file when loading testdata")
      call_db_load_large_event()
      do_default = False
    if options.db_testdata_insert:
      if not options.db_config_file: my_error("Please specify the db config file when loading testdata")
      call_db_testdata_insert()
      do_default = False
    if options.espresso_gen:
      if not options.espresso_data_file: my_error("Please specify the espresso data file for loading testdata")
      if not options.db_name: my_error("Please specify the espresso db name")
      if not options.db_table: my_error("Please specify the espresso table name")
      call_espresso_gen()
      do_default = False
    if do_default:   # no other options, do random event generation
      call_random_generator() 
    if options.wait_until_suspend:
      ret = wait_status_change("suspend", options.src_ids)  
    sys.exit(ret)

if __name__ == "__main__":
    main(sys.argv[1:])


