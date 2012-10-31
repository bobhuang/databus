#! /usr/bin/python
# -*- coding: iso-8859-1 -*-

import socket
import sys
import binascii
import struct
import time
import traceback,getopt

PORT=8999
HOST='127.0.0.1'
BATCH_SIZE=200
REC_LEN=400
NUM_BATCHES=10
SKIP_PRINT=False

FILTER = ''.join([(len(repr(chr(x))) == 3) and chr(x) or '.' for x in range(256)])

""" dump any string, ascii or encoded, to formatted hex output """
def dump_str(src, length=16):
    result = []
    for i in xrange(0, len(src), length):
       chars = src[i:i+length]
       hex = ' '.join(["%02x" % ord(x) for x in chars])
       printable = ''.join(["%s" % ((ord(x) <= 127 and FILTER[ord(x)]) or '.') for x in chars])
       result.append("%04x  %-*s  %s\n" % (i, length*3, hex, printable))
    return ''.join(result)


def version_cmp(v1, v2):
    parts1, parts2 = [map(int, v.split('.')) for v in [v1, v2]]
    parts1, parts2 = zip(*map(lambda p1,p2: (p1 or 0, p2 or 0), parts1, parts2))
    return cmp(parts1, parts2)


def send_payload(s,payload):
  s.sendall(struct.pack('<L' + str(len(payload)) + 's',
    len(payload),payload))
  
def do_quit(s):
  send_payload(s,"\x01")  

def do_get(s,db,table,start_pos, num_records,skip_first=False):
  db_len = len(db)
  table_len = len(table)
  fmt = '<BB' + str(db_len) + 'sB' + str(table_len) + 'sBBQL'
  mode = 0x3
  
  if skip_first:
    mode += 0x4
    
  payload = struct.pack(fmt, 2,db_len,db,table_len,table,0,mode,
    start_pos,num_records)
    
  print "Get for table " + db + "." + table
  print "payload_len="+str(len(payload))
  send_payload(s,payload)
  bytes_remaining = check_response(s)
  print "bytes_remaining=" + str(bytes_remaining)
  l = []
  
  while bytes_remaining > 0:
    l.append(s.recv(bytes_remaining))
    bytes_remaining -= len(l[-1])

  print dump_str(''.join(l))
  packet = ''.join(l)
  header = struct.unpack_from('<LQB', packet,0)
  print "%d records" % header[0]
  print "flags = %d " % header[2]
  return header[1]

def do_load(s,db,table,start_id, num_records,rec_len):
  db_len = len(db)
  table_len = len(table)
  fmt = '<BB' + str(db_len) + 'sB' + str(table_len) + 'sBL'
  val = 'a' * rec_len
  val = val[0:rec_len]
  l = [struct.pack(fmt, 0,db_len,db,table_len,table,0,num_records)]
  
  for i in range(start_id, start_id + num_records):
    l.append(struct.pack('<LH', i, rec_len))
    l.append(val)
  print "i=",i  
  payload = ''.join(l)
    
  print "Load for table " + db + "." + table
  print "payload_len="+str(len(payload))
  send_payload(s,payload)
  check_response(s)
  
def check_response(s):
  resp = s.recv(6)
  print "resp length = ", len(resp)
  arr =  struct.unpack_from('<HH',resp,0)
  print "Response code ",arr[0]

  if arr[0]:
    resp += s.recv(arr[1])
    arr = struct.unpack_from(str(arr[1]) + 's',resp,4) 
    print "Error Message: ", arr[0] 
    return 0  

  if len(resp) == 6:
    arr = struct.unpack_from('<L',resp, 2)
    return arr[0]
  else:
    return 0
  
if __name__ == "__main__":
    if version_cmp(sys.version.split(' ')[0], '2.6') < 0:
      print "Python version 2.6 or newer is required"
      sys.exit(1)
      
    try:
      opts, args = getopt.getopt(sys.argv[1:],'p:s',
          ['port=','skip-print'])
      
      for o,a in opts:
        if o in ('-p','--port'):
          PORT = int(a)
        elif o in ('-s','--skip-print'):
          SKIP_PRINT = True
      
    except:
      print "Error parsing options: " 
      exc_type, exc_value, exc_traceback = sys.exc_info()
      traceback.print_exception(exc_type, exc_value,
          exc_traceback,limit=2,file=sys.stdout)
      traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
      sys.exit(1)


s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setblocking(1)
s.connect((HOST,PORT))
do_load(s,"test","g1", 1, 10, 1000)
t_start = time.time()
for k in range(0,NUM_BATCHES):
  do_load(s,"test","t1", k*BATCH_SIZE + 1, BATCH_SIZE, REC_LEN)
t_run = time.time() - t_start

if t_run > 0 and not SKIP_PRINT:
  msg = "Test completed in %.2f seconds " % t_run
  msg += " %.2f MB/s" % ((NUM_BATCHES*BATCH_SIZE*REC_LEN)/(1024*1024*t_run))
  print msg 
 
pos=do_get(s,"test","t1",0,10) 
new_pos=do_get(s,"test","t1",pos,5)
do_get(s,"test","t1",pos,5,True)
pos=new_pos
pos=do_get(s,"test","t1",pos,40000)
do_get(s,"test","t1",pos,40000)
do_quit(s)

