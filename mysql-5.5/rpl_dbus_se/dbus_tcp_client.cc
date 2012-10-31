#include "dbus_tcp_client.h"
#include "log.h"
#include "my_sys.h"
#include "rpl_dbus_crc.h"


#define OP_HANDSHAKE ((uchar)0xA1)
#define OP_SEND_EVENT ((uchar)0xA2)
#define EVENT_MAGIC ((uchar)0)
#define HANDSHAKE_RESP_LEN 17

#define RESP_LEN 9

#define EVENT_HEADER_LEN 57
#define HEADER_CRC_POS 11
#define VALUE_CRC_POS (49 + PACKET_HEADER_LEN)
#define PAYLOAD_POS  EVENT_HEADER_LEN
#define HEADER_REAL_POS 5 
#define HEADER_REAL_LEN (EVENT_HEADER_LEN - HEADER_REAL_POS)
#define SCHEMA_ID_LEN 16
#define DBUS_PROTOCOL_VERSION  3
#define PACKET_HEADER_LEN 10
#define HANDSHAKE_LEN 6
#define EXTRA_LEN 8
#if HANDSHAKE_LEN > HANDSHAKE_RESP_LEN
#define HANDSHAKE_BUF_LEN HANDSHAKE_LEN
#else
#define HANDSHAKE_BUF_LEN HANDSHAKE_RESP_LEN
#endif

#define EOP_MASK 0x10
#define BYTE_KEY_MASK 0x08

extern ulong rpl_dbus_debug;

static void append_short(String* s, uint val);
static void append_ulonglong(String* s, ulonglong val);
static int reserve_length(String* s, uint len);

static int reserve_length(String* s, uint len)
{
  int res = 0;
  
  if (s->alloced_length() < len)
    res = s->realloc(len);
    
  return res;  
}


static void append_short(String* s, uint val)
{
  uint len = s->length();
  int2store(s->ptr() + len,val);
  s->length(len + 2);
}

static void append_ulonglong(String* s, ulonglong val)
{
  uint len = s->length();
  int8store(s->ptr() + len,val);
  s->length(len + 8);
}


int Dbus_tcp_client::shake_hands(Dbus_config *conf)
{
  uchar buf[4096];
  uint resp_len;
 
  String s((char*)buf,sizeof(buf),&my_charset_latin1);

  if (int ret = store_sources(conf, s))
    return ret;

  if (write(&s,s.length()))
  {
    store_error("Error on write during handshake", errno);
    return 1;
  }
  
  if (read((char*)buf,HANDSHAKE_RESP_LEN) != HANDSHAKE_RESP_LEN)
  {
    store_error("Bad response to handshake - wrong length or read error", errno);
    return 1;
  }
  
  if (buf[0] != 0)
  {
    char tmp[256];
    my_snprintf(tmp,sizeof(tmp),
      "Bad response to handshake - error code: %02d", (char)buf[0]); 
    store_error(tmp,0);
    return 1;
  }
  
  resp_len = uint4korr(buf+1);
  resp_lsn = uint8korr(buf+5);
  max_trans_size = uint4korr(buf+13);
  
  if (resp_lsn && resp_lsn < last_lsn)
  {
    lsn_change_requested = 1;
  }
  
  return 0;
}

int Dbus_tcp_client::store_sources(Dbus_config *conf, String& buffer)
{
  Dbus_source_entry *src;
  
  buffer.length(0);
  uint packet_len = sizeof(char) + sizeof(char) + sizeof(uint) + 
    sizeof(uint) + 
    (sizeof(uint16) + sizeof(uint16) + sizeof(src->src_name)) * 
      conf->get_num_sources();
      
  if (reserve_length(&buffer,packet_len))
  {
    store_error("Out of memory storing sources",errno);
    return 1;
  }

  buffer.q_append((char)OP_HANDSHAKE);
  buffer.q_append((char)DBUS_PROTOCOL_VERSION);
  buffer.q_append((uint)server_id);
  buffer.q_append((uint)conf->get_num_sources());

  Dbus_hash::srcIter src_iter;
  conf->init_src_iter(&src_iter);

  while ((src = src_iter.get_next_source()))
  {
    append_short(&buffer, src->src_id);
    append_short(&buffer, src->src_len);
    buffer.append(src->src_name, src->src_len);
  }

  return 0;  
}

int Dbus_tcp_client::store_event(Dbus_event* e)
{
  uint event_len = EVENT_HEADER_LEN + e->val_len + e->key_len;
  
  if (reserve_length(&buffer,event_len + EXTRA_LEN))
  {
    store_error("Out of memory storing event",errno);
    return 1;
  }
  
  buffer.length(0);
  buffer.q_append((char)OP_SEND_EVENT);
  buffer.q_append((char)DBUS_PROTOCOL_VERSION);
  append_ulonglong(&buffer,e->lsn); // sequence
  buffer.q_append((char)EVENT_MAGIC);
  buffer.q_append((uint32)0); // place-holder for crc
  
  buffer.q_append(event_len);
  append_short(&buffer,e->opcode | EOP_MASK | BYTE_KEY_MASK);
  
  append_ulonglong(&buffer,e->binlog_id); // sequence
  append_short(&buffer,e->phys_part_id);
  append_short(&buffer,e->l_part_id);
  append_ulonglong(&buffer,e->ns_ts); // nanoTimestamp
  append_short(&buffer,e->src_id);
  
  if (e->schema_id)
    buffer.q_append(e->schema_id,SCHEMA_ID_LEN); // MD5 sum of schema_file
  else
  {
    // the buffer will have the space allocated, so it is OK
    // to remember and write to pointers
    char* p = (char*)buffer.ptr() + buffer.length();
    memset(p,0,SCHEMA_ID_LEN);
    buffer.length(buffer.length() + SCHEMA_ID_LEN);
  }
      
  buffer.q_append((uint32)0);
  buffer.q_append((uint32)e->key_len);
  
  if (e->key && e->key_len)
    buffer.q_append(e->key,e->key_len);
  
  if (e->val && e->val_len)
    buffer.q_append(e->val,e->val_len);
    
  buffer.write_at_position(VALUE_CRC_POS,
    (uint32)rpl_dbus_crc((uchar*)buffer.ptr() + PAYLOAD_POS + 
    PACKET_HEADER_LEN, 
    e->val_len + e->key_len));
  buffer.write_at_position(HEADER_CRC_POS,
    (uint32)rpl_dbus_crc((uchar*)buffer.ptr() + HEADER_REAL_POS + 
    PACKET_HEADER_LEN, 
    HEADER_REAL_LEN));
  
  check_resp_lsn = e->lsn;
  
  if (rpl_dbus_debug)
    sql_print_information("D-BUS: sending lsn=%llu", e->lsn);
  return 0;  
}

int Dbus_tcp_client::read_response()
{
  uchar buf[RESP_LEN];
  int bytes_read;
  lsn_change_requested = 0;
  
  if ((bytes_read=read((char*)buf,RESP_LEN)) != RESP_LEN)
  {
    char buf[256];
    my_snprintf(buf,sizeof(buf),
      "Bad response in event ACK during read:"
      " read %d bytes instead of %d", bytes_read, RESP_LEN);
    store_error(buf, errno);
    return 1;
  }
  
  if (buf[0] != 0)
  {
    store_error("D-BUS server responded with error code: ", buf[0]);
    return 1;
  }
  
  /*
   comment this out for now - protocol change, but leave the code
   around just in case
  resp_lsn = uint8korr(buf+1);
  
  if (resp_lsn && resp_lsn < check_resp_lsn)
  {
    sql_print_information(
     "D-BUS server responded with a smaller"
     " sequence number - expected: %llu, got: %llu"
     " - initiatining LSN change", check_resp_lsn,
      resp_lsn); 
      
    lsn_change_requested = 1;  
  } 
  */
  return 0;
}
