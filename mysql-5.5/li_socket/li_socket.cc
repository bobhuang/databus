#include <my_pthread.h>                         

#define MYSQL_SERVER 1
#include <sql_priv.h>
#include <my_global.h>
#include <log.h>
#include <stdlib.h>
#include <ctype.h>
#include <mysql_version.h>
#include <mysql/plugin.h>
#include "my_sys.h"                             
#include "m_string.h"                           
#include "sql_plugin.h"   
#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>
#include "sql_class.h"
#include "sql_string.h"
#include "sql_base.h"

#include "transaction.h"
#include "log_event.h"
#include "rpl_record.h"

#include "../storage/rpl_dbus/tcp_socket.h"
#include "li_socket.h"


static struct st_mysql_daemon li_socket_plugin = {
  MYSQL_DAEMON_INTERFACE_VERSION
};

extern my_bool opt_bootstrap;

static char *li_socket_address = 0;
static unsigned int li_socket_port = 0;
static unsigned int li_socket_epoll = 1;
static unsigned int li_socket_threads = 32;
static unsigned int li_socket_read_timeout = 5;
static unsigned int li_socket_write_timeout = 5;
static unsigned int li_socket_idle_timeout = 1200;
static unsigned int li_socket_backlog = 32768;
static unsigned int li_socket_max_allowed_packet = 0;
static unsigned int li_socket_accept_balance = 0;
static char *li_socket_plain_secret = 0;
static uint li_socket_verbose_level = 10;
static int li_socket_fd = -1;
static struct sockaddr_in li_socket_addr;
static mysql_mutex_t LOCK_li_socket_count;
static mysql_cond_t COND_li_socket_count;
static uint li_socket_count = 0;
static bool li_socket_requested_close = 0;

static MYSQL_SYSVAR_UINT(verbose, li_socket_verbose_level, 0,
  "0..10000", 0, 0, 10 /* default */, 0, 10000, 0);
static MYSQL_SYSVAR_UINT(epoll, li_socket_epoll, PLUGIN_VAR_READONLY,
  "0..1", 0, 0, 1 /* default */, 0, 1, 0);
static MYSQL_SYSVAR_STR(address, li_socket_address,
  PLUGIN_VAR_READONLY | PLUGIN_VAR_MEMALLOC, "", NULL, NULL, NULL);
static MYSQL_SYSVAR_UINT(port, li_socket_port,
  PLUGIN_VAR_READONLY | PLUGIN_VAR_MEMALLOC, "1..65535", 
   0, 0, LI_SOCKET_PORT/* default */, 1, 65535, 0);

static MYSQL_SYSVAR_UINT(threads, li_socket_threads, PLUGIN_VAR_READONLY,
  "1..3000", 0, 0, 16 /* default */, 1, 3000, 0);
static MYSQL_SYSVAR_UINT(read_timeout, 
                         li_socket_read_timeout, PLUGIN_VAR_READONLY,
  "Read timeout", 0, 0, 5 /* default */, 1, 3600, 0);
static MYSQL_SYSVAR_UINT(write_timeout, 
                         li_socket_write_timeout, PLUGIN_VAR_READONLY,
  "Write timeout", 0, 0, 5 /* default */, 1, 3600, 0);
static MYSQL_SYSVAR_UINT(idle_timeout, 
                         li_socket_idle_timeout, PLUGIN_VAR_READONLY,
  "Write timeout", 0, 0, 1200 /* default */, 1, 3600, 0);

static MYSQL_SYSVAR_UINT(backlog, li_socket_backlog, PLUGIN_VAR_READONLY,
  "5..1000000", 0, 0, 32768 /* default */, 5, 1000000, 0);
static MYSQL_SYSVAR_UINT(max_allowed_packet, 
                         li_socket_max_allowed_packet, PLUGIN_VAR_READONLY,
  "Maximum packet size", 0, 0, 128 * 1024 * 1024 /* default */, 
  0, 512 * 1024 * 1024, 0);
static MYSQL_SYSVAR_UINT(accept_balance, li_socket_accept_balance,
  PLUGIN_VAR_READONLY, "0..10000", 0, 0, 0 /* default */, 0, 10000, 0);
static MYSQL_SYSVAR_STR(plain_secret, li_socket_plain_secret,
  PLUGIN_VAR_READONLY | PLUGIN_VAR_MEMALLOC, "", NULL, NULL, NULL);

static struct st_mysql_sys_var *li_socket_system_variables[] = {
  MYSQL_SYSVAR(verbose),
  MYSQL_SYSVAR(address),
  MYSQL_SYSVAR(port),
  MYSQL_SYSVAR(epoll),
  MYSQL_SYSVAR(threads),
  MYSQL_SYSVAR(backlog),
  MYSQL_SYSVAR(accept_balance),
  MYSQL_SYSVAR(read_timeout),
  MYSQL_SYSVAR(write_timeout),
  MYSQL_SYSVAR(idle_timeout),
  MYSQL_SYSVAR(plain_secret),
  MYSQL_SYSVAR(max_allowed_packet),
  0
};

static void* li_socket_run_loop(void* arg);

static SHOW_VAR li_s_status_variables[] = {
  {NullS, NullS, SHOW_LONG}
};

extern "C" void __cxa_pure_virtual() { return; }
static int show_li_s_vars(THD *thd, SHOW_VAR *var, char *buff)
{
  var->type= SHOW_ARRAY;
  var->value= (char *) &li_s_status_variables;
  return 0;
}


static SHOW_VAR li_socket_status_variables[] = {
  {"Li_s", (char*) show_li_s_vars, SHOW_FUNC},
  {NullS, NullS, SHOW_LONG}
};

static void li_socket_process_request(Li_socket_conn* conn);


#ifdef HAVE_PSI_INTERFACE
static PSI_mutex_key key_mutex_li_socket;
static PSI_cond_key key_cond_li_socket;
static PSI_thread_key key_thread_li_socket_thread;

static PSI_thread_info all_li_socket_threads[]=
{
  { &key_thread_li_socket_thread, "li_socket", PSI_FLAG_GLOBAL},
};

static PSI_cond_info all_li_socket_conds[]=
{
  { &key_cond_li_socket, "li_socket", PSI_FLAG_GLOBAL},
};


static PSI_mutex_info all_li_socket_mutexes[]=
{
  { &key_mutex_li_socket, "rpl_dbus", PSI_FLAG_GLOBAL},
};

void init_li_socket_psi_keys()
{
  const char* category= "li_socket";
  int count;

  if (PSI_server == NULL)
    return;

  count= array_elements(all_li_socket_mutexes);
  PSI_server->register_mutex(category, all_li_socket_mutexes, count);
  count = array_elements(all_li_socket_threads);
  PSI_server->register_thread(category, all_li_socket_threads, count);
  count = array_elements(all_li_socket_conds);
  PSI_server->register_cond(category, all_li_socket_conds, count);
}
#endif


void Li_socket_conn::close()
{
  if (sock)
    delete sock;
  
  sock = 0;
  fd = -1;
}

int Li_socket_conn::read(uchar* buf, uint len)
{
   return sock->read_fully((char*)buf,len);
}

int Li_socket_conn::write(uchar* buf, uint len)
{
  return sock->write((char*)buf,len);
}

int Li_socket_conn::send_error(uint16 error_code,const char* msg)
{
  if (!sock) // replication SQL slave thread
  {
    strncpy(err_msg,msg,sizeof(err_msg));
    return 0;
  }
  
  uchar* buf = (uchar*)my_alloca(LI_SOCKET_ERR_LEN + strlen(msg));
  uint msg_len = strlen(msg);
  int2store(buf,error_code);
  int2store(buf+2,msg_len);
  memcpy(buf+4,msg,msg_len);
  int res = write(buf, LI_SOCKET_ERR_LEN + msg_len);
  my_afree(buf);
  
  if (error_code == LI_SOCKET_ERR_FATAL)
    close();
  
  return res;
}

int Li_socket_conn::send_ok()
{
  uchar buf[LI_SOCKET_OK_LEN];
  int2store(buf,0);
  int2store(buf+2,0);
  int res = write(buf,sizeof(buf));
  
  if (res)
    close();
  
  return res;
}

#define CHECK_OVERRUN(msg,extra) if (buf + extra >= buf_end) \
  { send_error(LI_SOCKET_ERR_OP,msg); return -1;}

#define CHECK_OVERRUN_WITH_GOTO(msg,extra) if (buf + extra >= buf_end) \
  { send_error(LI_SOCKET_ERR_OP,msg); res = -1; goto err; }

int Li_socket_conn::parse_table_and_open(uint len)
{
  int res = 0;
  
  if (len < 3)
  {
    send_error(LI_SOCKET_ERR_OP, "Payload length too small");
    return -1;
  }
  
  buf++; // skip command
  db_len = *buf++;
  
  db_name = (char*)buf;
  buf += db_len;
  
  CHECK_OVERRUN("Packet overrun on database name check",1)
  table_len = *buf;
  table_len_pos = buf;
  *buf++ = 0;
  table_name = (char*)buf;
  buf += table_len;
  CHECK_OVERRUN("Packet overrun on table name check",5)
  
  // sacrificial byte reserved by the client for efficiency
  *buf++ = 0;
  
  tbl.init_one_table(db_name,db_len,table_name,table_len, table_name,
                     TL_WRITE);
  
  tbl.open_type = OT_BASE_ONLY;
  tbl.open_strategy = TABLE_LIST::OPEN_NORMAL;
  
  thd->variables.sql_log_bin = 0;
  
  if (open_and_lock_tables(thd, &tbl, 
       FALSE, MYSQL_LOCK_IGNORE_TIMEOUT) || !tbl.table || !tbl.table->s)
  {
    res = -1;
    send_error(LI_SOCKET_ERR_OP, "Error opening table");
    goto err;
  }     
  
  tbl.table->use_all_columns();
  
err:
  return res;
}

int Li_socket_conn::buffer_current_record()
{
  uint max_rec_len = max_row_length(tbl.table, tbl.table->record[0]);    
  
  if (secure_record_buffer(record_buffer_ptr - record_buffer + 
      max_rec_len))
  {
    send_error(LI_SOCKET_ERR_OP, "Out of memory allocating record buffer");
    return -1;
  }
  
  record_buffer_ptr += pack_row(tbl.table, tbl.table->read_set, 
    record_buffer_ptr, tbl.table->record[0]);

  return 0;
}

int Li_socket_conn::secure_record_buffer(uint buf_size)
{
  if (record_buffer_size > buf_size)
    return 0;
  
  if (buf_size - record_buffer_size < LI_SOCKET_RECORD_BUFFER_PAD)
  {
    buf_size = record_buffer_size + LI_SOCKET_RECORD_BUFFER_PAD;
  }
  
  uint current_offset = (record_buffer) ? record_buffer_ptr - record_buffer 
   : 0;
  
  if (!(record_buffer = (uchar*)my_realloc(record_buffer, buf_size,
      MYF(MY_ALLOW_ZERO_PTR))))
  {
    record_buffer_size = 0;
    record_buffer_ptr = 0;
    return -1;
  }
  
  record_buffer_ptr = record_buffer + current_offset;
  record_buffer_size = buf_size;
  return 0;
}

uint Li_socket_conn::guess_rec_size()
{
  return tbl.table->file->stats.mean_rec_length * 3/2 
   /* fudge factor to reduce the chance of having to realloc the buffer*/;
}


int Li_socket_conn::handle_get(uchar* buf_arg, uint len)
{
  buf = buf_arg;
  buf_end = buf + len;
  int res = 0;
  uchar mode;
  uchar* pos;
  uint size_is_in_records;
  uint32 read_size;
  uint init_buffer_size;
  uint records = 0;
  bool done = 0;
  
  handler* h;
  bool done_rnd_init = 0;
  int error;
  int skip_first_record = 0;
  uchar resp_flags = 0;
  
  thd->thread_stack = (char*)&res;
  
  if ((res=parse_table_and_open(len)))
    goto err;
  
  CHECK_OVERRUN_WITH_GOTO("Packet overrun in GET", 
                           LI_SOCKET_GET_REQ_HEADER_SIZE-1)
  mode = *buf++;
  
  if (!(mode & LI_SOCKET_POS_LOOKUP))
  {
    send_error(LI_SOCKET_ERR_OP, "PK lookup not yet implemented");
    res = -1;
    goto err;
  }
  
  size_is_in_records =  (mode & LI_SOCKET_SIZE_IN_RECORDS);
  skip_first_record = (mode & LI_SOCKET_SKIP_FIRST_RECORD);
  
  h = tbl.table->file;
  pos = buf;
  buf += 8;
  read_size = uint4korr(buf);
  init_buffer_size = (size_is_in_records) ? read_size * guess_rec_size() + 
   LI_SOCKET_GET_RESP_HEADER_SIZE : read_size;
  
  if (secure_record_buffer(init_buffer_size))
  {
    send_error(LI_SOCKET_ERR_OP, "Not enough memory for the record buffer");
    res = -1;
    goto err;
  }
  
  record_buffer_ptr = record_buffer + LI_SOCKET_GET_RESP_HEADER_SIZE;
  
  if (h->ha_rnd_init(1))
  {
    send_error(LI_SOCKET_ERR_OP,"Error initiating table read");
    res = -1;
    goto err;
  }
  
  done_rnd_init = 1;
  
  if ((error = h->rnd_pos(tbl.table->record[0],pos)) && 
      error != HA_ERR_RECORD_DELETED)
  {
    send_error(LI_SOCKET_ERR_OP,"Error reading from position");
    res = -1;
    goto err;
  }
  
  if (error || skip_first_record) // record is deleted or skip requested
    goto read_next;
  
  for (;!done;)
  {
    if (buffer_current_record())
      goto err; /* buffer_current_record() sends the message */
    
    records++;
    
    if (size_is_in_records)
    {  
      if (records >= read_size)
        done = 1;
    }  
    else
    {
      if (record_buffer_ptr - record_buffer >= read_size)
        done = 1;
    }
    
 
 read_next:
    switch (h->rnd_next(tbl.table->record[0]))
    {
      case HA_ERR_END_OF_FILE:
      {
        resp_flags = LI_SOCKET_EOF;
        h->position(tbl.table->record[0]);
        store_position(h->ref,h->ref_length);
        done = 1;
        break;
      } 
      case HA_ERR_RECORD_DELETED: /* skip */
        goto read_next;
      case 0: /* success */
        if (done)
        {
          h->position(tbl.table->record[0]);
          store_position(h->ref,h->ref_length);
        }
        break;
      default:  
        send_error(LI_SOCKET_ERR_OP,"Error reading the next record");
        goto err;
    }
  }
  
  if (done_rnd_init)
    h->ha_rnd_end();
    
err:
  if (thd)
  {
    close_thread_tables(thd);
  }

  if (res == 0) /* success */
  {
    int2store(record_buffer,0);
    int4store(record_buffer+2,(uint32)(record_buffer_ptr - record_buffer 
      - 6)); /* remaining packet length */
    int4store(record_buffer+6,records);
    record_buffer[18] = resp_flags;
    
    if (write(record_buffer, record_buffer_ptr - record_buffer))
    {
      close();
      return -1;
    }  
  }
  
  return res;
}

void Li_socket_conn::store_position(uchar* pos, uint len)
{
  uchar* pos_ptr = record_buffer + 10;
    
  memcpy(pos_ptr,pos,len);  
  if (len < 8)
    bzero(pos_ptr + len, 8 - len);
}


int Li_socket_conn::handle_load(uchar* buf_arg, uint len)
{
  buf = buf_arg;
  buf_end = buf + len;
  int res = 0;
  table_len_pos = 0;
  thd->thread_stack = (char*)&res;
  
  if ((res=parse_table_and_open(len)))
    goto err;
  
   // block to make h invisible past err label  
  {
    uint num_rows = uint4korr(buf);
    buf += 4;
    handler* h = tbl.table->file;
    h->ha_start_bulk_insert(num_rows);
    h->extra(HA_EXTRA_WRITE_CAN_REPLACE);
    h->extra(HA_EXTRA_IGNORE_DUP_KEY);
    
    for (;buf < buf_end;)
    {
      Field* f, **f_ptr, **fields_end;
      uint field_len;
      fields_end = tbl.table->field + tbl.table->s->fields;
      restore_record(tbl.table, s->default_values);
      int error;
      
      for (f_ptr = tbl.table->field; f_ptr < fields_end; f_ptr++)
      {
        f = *f_ptr;
        switch (f->type())
        {
          case MYSQL_TYPE_BLOB:
          {
            Field_blob* fb = (Field_blob*)f;
            uint packlen = fb->pack_length_no_ptr();
            CHECK_OVERRUN_WITH_GOTO("Packet overrun on blob field length", 
               packlen)
            switch (packlen)
            {
              case 1: field_len = *buf; break;
              case 2: field_len = uint2korr(buf); break;
              case 3: field_len = uint3korr(buf); break;
              case 4: field_len = uint4korr(buf); break;
              default: 
                send_error(LI_SOCKET_ERR_FATAL,"Invalid packlen in BLOB"); 
                goto err;
            }
            
            
            if (field_len)
            {
              CHECK_OVERRUN_WITH_GOTO("Packet overrun on blob field", 
               packlen + field_len - 1)
              fb->set_ptr(buf, buf + packlen);
            }
            buf += packlen + field_len;
            break;
          }  
          case MYSQL_TYPE_VARCHAR:
            CHECK_OVERRUN_WITH_GOTO("Packet overrun on varchar field length", 
               1)
            if ((field_len = *buf++))
            {
              CHECK_OVERRUN_WITH_GOTO("Packet overrun on varchar field", 
                field_len - 1)
              f->store((char*)buf,field_len,f->charset());
              buf += field_len;
            }  
            break;
          default:
            CHECK_OVERRUN_WITH_GOTO("Packet overrun on fixed length field", 
               f->pack_length() - 1)
            f->set_image(buf, f->pack_length(), f->charset());
            buf += f->pack_length();
            break;
        }
      }
      
      if ((error = h->ha_write_row(tbl.table->record[0])))
      {
        res = -1;
        send_error(LI_SOCKET_ERR_OP, "Error inserting record");
        goto err;
      }
    }
    
    h->ha_end_bulk_insert();
    h->extra(HA_EXTRA_WRITE_CANNOT_REPLACE);
    h->extra(HA_EXTRA_NO_IGNORE_DUP_KEY);

#ifndef EMBEDDED_LIBRARY
    if (mysql_bin_log.is_open())
    {
      *table_len_pos = table_len; /* restore for proper binlogging */
      Li_socket_load_log_event e(thd,packet,packet_len);
      
      if (mysql_bin_log.write(&e))
      {
        res = -1;
        send_error(LI_SOCKET_ERR_OP, "Error writing to binlog");
        goto err;
      }
    }
#endif    
  }
err:
  if (thd)
  {
    if (trans_commit_stmt(thd))
    {
      res = -1;
      send_error(LI_SOCKET_ERR_OP, "Error in commit");
    }
    close_thread_tables(thd);
  }
  
  return res;
}

static void* li_socket_run_loop(void* arg)
{
  my_thread_init();
  
  mysql_mutex_lock(&LOCK_li_socket_count);
  li_socket_count++;
  mysql_mutex_unlock(&LOCK_li_socket_count);
  
  for (;;)
  {
    Li_socket_conn conn; 
    conn.id = (uint)(uint64)arg;
    
    socklen_t len = sizeof(conn.addr);
    conn.fd = accept(li_socket_fd, (struct sockaddr*)&conn.addr, 
        &len);
    
    if (abort_loop || li_socket_requested_close)
    {  
      conn.close();
      break;
    }
    
    if (conn.fd < 0)
    {
      sql_print_warning("LI_SOCKET: error in accept(): (%d)", errno);
      continue;
    }
    
    if ((conn.sock = new Tcp_socket(0,0,conn.fd)))
    {
      for (;;)
      {  
        conn.set_write_timeout(li_socket_write_timeout);
        li_socket_process_request(&conn);
        
        if (!conn.is_open() || abort_loop || li_socket_requested_close)
          break;
      }  
    }  
    else
      sql_print_error("LI_SOCKET: out of memory");
    conn.close();
  }
  
  
  mysql_mutex_lock(&LOCK_li_socket_count);
  li_socket_count--;
  mysql_cond_broadcast(&COND_li_socket_count);
  mysql_mutex_unlock(&LOCK_li_socket_count);
  my_thread_end();
  return 0;
}

static bool li_socket_disabled()
{
  return li_socket_port == 0 || opt_bootstrap;
}

static int li_socket_init(void* p)
{
  uint i;
  
  if (li_socket_disabled())
    return 0;
  
#ifdef HAVE_PSI_INTERFACE
  init_li_socket_psi_keys();
#endif
  
  mysql_mutex_init(key_mutex_li_socket, &LOCK_li_socket_count,
                   MY_MUTEX_INIT_FAST);
  
  mysql_cond_init(key_cond_li_socket, &COND_li_socket_count,
       NULL );

  if (li_socket_listen())
  {
    sql_print_error("LI_SOCKET: cannot listen on port");
    return -1;
  }

  for (i = 0; i < li_socket_threads; i++)
  {
    pthread_t th;
    
    if (mysql_thread_create(key_thread_li_socket_thread, &th, 
                        &connection_attrib, li_socket_run_loop,
                        (void*)i
                       ))
    {
      sql_print_warning("LI_SOCKET: error spawning thread #%d (%d)",
                        i, errno);
    }
  }

  return 0;
}

static int li_socket_deinit(void* p)
{
  if (li_socket_disabled())
    return 0;
  
  li_socket_requested_close = 1;
  li_socket_close();
  
  mysql_mutex_lock(&LOCK_li_socket_count);
  
  while (li_socket_count)
    mysql_cond_wait(&COND_li_socket_count, &LOCK_li_socket_count);
  
  mysql_mutex_unlock(&LOCK_li_socket_count);
  
  mysql_mutex_destroy(&LOCK_li_socket_count);
  mysql_cond_destroy(&COND_li_socket_count);
  return 0;
}

int li_socket_listen()
{
  if ((li_socket_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
  {
    sql_print_error("LI_SOCKET: Error creating socket: (%d)", errno);  
    return -1;
  }
  
  bzero((char*)&li_socket_addr, sizeof(li_socket_addr));
  li_socket_addr.sin_family = AF_INET;
  li_socket_addr.sin_addr.s_addr = INADDR_ANY;
  li_socket_addr.sin_port = htons(li_socket_port);
  
  int arg = 1;
  (void) setsockopt(li_socket_fd,SOL_SOCKET,SO_REUSEADDR,
                    (char*)&arg,sizeof(arg));
  
  if (bind(li_socket_fd, (struct sockaddr *) &li_socket_addr,
       sizeof(li_socket_addr)) < 0)
  {
    sql_print_error("LI_SOCKET: Error in bind(): (%d)", errno);
    return -1;
  }
  
  if (listen(li_socket_fd, li_socket_backlog))
  {
    sql_print_error("LI_SOCKET: Error in listen(): (%d)", errno);
    return -1;
  }
  
  return 0;
}

int li_socket_close()
{
  if (li_socket_fd >= 0)
  {
     shutdown(li_socket_fd, SHUT_RDWR);
     close(li_socket_fd);
     li_socket_fd = -1;
  }
  
  return 0;
}

static void li_socket_process_request(Li_socket_conn* conn)
{
  uchar len_buf[4];
  
  conn->set_read_timeout(li_socket_idle_timeout);
  if (conn->read(len_buf,sizeof(len_buf)))
  {
    sql_print_error("LI_SOCKET: Error reading payload length");
    conn->close();
    return;
  }
  
  uint payload_len = uint4korr(len_buf);
  if (payload_len > li_socket_max_allowed_packet)
  {
    conn->send_error(LI_SOCKET_ERR_FATAL, "Packet too large");
    return;
  }
  
  if (conn->packet_len < payload_len)
  {
    if (!(conn->packet = (uchar*)my_realloc(conn->packet,payload_len,
      MYF(MY_ALLOW_ZERO_PTR))))
    {
      conn->send_error(LI_SOCKET_ERR_FATAL, 
                        "Out of memory allocating read buffer");
      return;
    }
    
    conn->packet_len = payload_len;
  }
  
  conn->set_read_timeout(li_socket_read_timeout);
  
  if (conn->read(conn->packet,payload_len))
  {
    conn->send_error(LI_SOCKET_ERR_FATAL, "Error reading payload");
    return;
  }

  if (!conn->thd && conn->init_thd())
  {
    conn->send_error(LI_SOCKET_ERR_OP, "Error initializing THD");
    return;
  }

  switch (*conn->packet)
  {
    case LI_SOCKET_CMD_LOAD:
      if (conn->handle_load(conn->packet,payload_len))
      {
        return;
      }
      conn->send_ok();
      break;
      
    case LI_SOCKET_CMD_GET:
      if (conn->handle_get(conn->packet,payload_len))
      {
        return;
      }
      //handle_get() sends its own response
      break;
   
    case LI_SOCKET_CMD_QUIT:
      conn->close();
      return;
      
    default:
      conn->send_error(LI_SOCKET_ERR_OP, "Invalid command");
      sql_print_error("Invalid command %u, payload_len = %u", 
                      *conn->packet, payload_len);
      return;
  }
  
}

mysql_declare_plugin(li_socket)
{
  MYSQL_DAEMON_PLUGIN,
  &li_socket_plugin,
  "li_socket",
  "Sasha Pachev",
  "",
  PLUGIN_LICENSE_BSD,
  li_socket_init,
  li_socket_deinit,
  0x0100 /* 1.0 */,
  li_socket_status_variables,
  li_socket_system_variables,
  0
}
mysql_declare_plugin_end;
