#! /usr/bin/python
# -*- coding: iso-8859-1 -*-

import SocketServer
import sys
import struct
import traceback
import binascii
import os
import getopt

OP_HANDSHAKE = 0xA1
OP_SEND_EVENT = 0xA2
EVENT_MAGIC=0x00
HANDSHAKE_IN_LEN=6
HANDSHAKE_OUT_LEN=17
HEADER_CRC_POS=1
HEADER_LEN=57
EVENT_HEADER_LEN=10
EVENT_LEN_POS=EVENT_HEADER_LEN+5
TEST_LSN_COUNT=5
MAX_EVENTS=9

def create_pid_file(fname):
  try:
    pid=os.getpid()
    f = open(fname,"w+")
    f.write(str(pid))
    f.close()
  except:
    print "Error creating pid file " + fname
    sys.exit(1)

def uint_cast(n):
  return n & 0xffffffff

class Dbus_event:
  def __init__(self,data,payload_len):
       debug_print("data_len="+str(len(data)))
       header = data[EVENT_HEADER_LEN:HEADER_LEN+EVENT_HEADER_LEN];
       debug_print("header_len="+str(len(header)))
       fmt = '<BLLHQHHQH16sLL'
       #print "fmt="+fmt
       arr =  struct.unpack_from(fmt,header,0) 
       self.magic = arr[0]
       self.header_crc = arr[1]
       self.event_len = arr[2]
       self.opcode = arr[3]
       self.lsn = arr[4]
       self.phys_part_id = arr[5]
       self.l_part_id = arr[6]
       self.ns_ts = arr[7]
       self.src_id = arr[8]
       self.schema_id = arr[9]
       self.val_crc = arr[10]
       self.key_len = arr[11]
       self.val_pos = EVENT_HEADER_LEN+HEADER_LEN+self.key_len
       fmt = str(self.key_len) + 's' + str(payload_len-self.key_len) + 's'
       arr = struct.unpack_from(fmt,data,HEADER_LEN+EVENT_HEADER_LEN)
       self.key = arr[0];
       self.val = arr[1]
       
  def verify_crc(self,data):
    calc_crc = uint_cast(
       binascii.crc32(
         data[EVENT_LEN_POS:HEADER_LEN+EVENT_HEADER_LEN]))
    if  calc_crc != self.header_crc:
        print "Wrong CRC for the header, expected " + str(self.header_crc) + ", got " + str(calc_crc)
        return 1
        
    print "Header CRC OK"
    debug_print("val_pos="+str(self.val_pos))
    calc_crc = uint_cast(
       binascii.crc32(data[self.val_pos:]))
    if  calc_crc != self.val_crc:
        print "Wrong CRC for the value, expected " + str(self.val_crc) + ", got " + str(calc_crc)
        return 1
    
    return 0    

  def do_print(self):
    print "[start event]"
    #print "header_crc=" + str(self.header_crc)
    print "opcode=" + str(self.opcode & 0x3)
    print "lsn=" + str(self.lsn)
    print "phys_part_id=" + str(self.phys_part_id)
    print "l_part_id=" + str(self.l_part_id)
    print "schema_id=" + binascii.hexlify(self.schema_id)
    print "key="+binascii.b2a_hex(self.key)
    print "val="+self.val
    print "[end event]"
    sys.stdout.flush()

def debug_print(msg):
       return
       sys.stderr.write("Debug: " + msg+"\n")

class Dbus_tcp_handler(SocketServer.BaseRequestHandler):

    def __init__(self,request, client_address, server):
      self.target_lsn = (1 << 63);
      self.test_lsn_counter = TEST_LSN_COUNT
      SocketServer.BaseRequestHandler.__init__(self, request,
         client_address, server)
       
    def shake_hands(self):
      try:
        self.data = self.request.recv(HANDSHAKE_IN_LEN)
        debug_print("read " + str(len(self.data)) + " bytes")
        self.op_code,self.version,self.server_id = \
           struct.unpack('<BBL',self.data)
       
        debug_print("op_code="+hex(self.op_code))    
        if self.op_code != OP_HANDSHAKE:
          print "Bad opcode: " + str(self.op_code)
          return 1
        self.data = struct.pack('<BLQL',0,
           HANDSHAKE_OUT_LEN,self.target_lsn,0)
        self.request.send(self.data)  
        print "Handshake successful, server_id=" + str(self.server_id)
        return 0  
      except:
        print "Exception during handshake:"
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value,
          exc_traceback,limit=2,file=sys.stdout)
        traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
        traceback.print_tb(exc_traceback, limit=1, file=sys.stderr)

        return 1
        
    def handle_event(self):
      self.data = self.request.recv(1024)
      self.len_read = len(self.data)

      if self.len_read == 0:
        print "Read zero bytes"
        return 1
        
      self.op_code,self.protocol_version,self.lsn = \
       struct.unpack('<BBQ',self.data[0:EVENT_HEADER_LEN])
      
      if self.op_code != OP_SEND_EVENT:
        print "Bad opcode in event: opcode="+str(self.magic)
        return 1
      
      self.event_len, = struct.unpack_from('<L',
             self.data,EVENT_LEN_POS)
      
      self.len_read = self.len_read - EVENT_HEADER_LEN
              
      while self.len_read < self.event_len:
        self.bytes_left = self.event_len - self.len_read
        self.data = self.data + self.request.recv(self.bytes_left)
        self.len_read = len(self.data)
      
      self.payload_len = self.event_len - HEADER_LEN  
      self.e = Dbus_event(self.data,self.payload_len)
      err_code = self.e.verify_crc(self.data)
      debug_print("err_code="+str(err_code))
      self.e.do_print()
      
      lsn_to_send = self.e.lsn

      if self.test_lsn_counter == TEST_LSN_COUNT - 2:
        self.target_lsn = lsn_to_send
      
      lsn_msg_extra = ""
      
      if self.test_lsn_counter == 0:
        self.test_lsn_counter = TEST_LSN_COUNT
        lsn_to_send = self.target_lsn
        lsn_msg_extra = ", LSN reset requested"
      
      lsn_msg = "Ack with lsn=" + str(lsn_to_send) + lsn_msg_extra
      print lsn_msg
      
      self.test_lsn_counter -= 1
      self.data = struct.pack('<BL',err_code,1)
      self.request.send(self.data)  
      
    def handle(self):
        debug_print("Handling handshake")
        if self.shake_hands():
          debug_print("bad handshake")
          print "Bad handshake"
          return 1
        for i in range(MAX_EVENTS):
          if self.handle_event():
            print "Error handling event"
            return 1
        print "D-BUS testing disconnect after " + str(MAX_EVENTS) + " events"    
        return 0  

def usage():
  print "Usage: " + sys.argv[0] + " --port=PORT --pid-file=PID_FILE"
  sys.exit(1)

def version_cmp(v1, v2):
    parts1, parts2 = [map(int, v.split('.')) for v in [v1, v2]]
    parts1, parts2 = zip(*map(lambda p1,p2: (p1 or 0, p2 or 0), parts1, parts2))
    return cmp(parts1, parts2)
             
if __name__ == "__main__":
    if version_cmp(sys.version.split(' ')[0], '2.6') < 0:
      print "Python version 2.6 or newer is required"
      sys.exit(1)
      
    try:
      opts, args = getopt.getopt(sys.argv[1:],'p:P:',
          ['port=','pid-file='])
      port = None
      pid_file = None
      
      for o,a in opts:
        if o in ('-p','--port'):
          port = int(a)
        elif o in ('-P','--pid-file'):
          pid_file = a
      
      if port == None or pid_file == None:
        usage()
    except:
      print "Error parsing options: " 
      exc_type, exc_value, exc_traceback = sys.exc_info()
      traceback.print_exception(exc_type, exc_value,
          exc_traceback,limit=2,file=sys.stdout)
      traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
      sys.exit(1)
    
    HOST, PORT = "localhost", port 

    server = SocketServer.TCPServer((HOST, PORT), Dbus_tcp_handler)
    create_pid_file(pid_file)
    server.serve_forever()




