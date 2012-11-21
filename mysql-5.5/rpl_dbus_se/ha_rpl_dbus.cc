/* Copyright 2005-2008 MySQL AB, 2008-2009 Sun Microsystems, Inc.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */


#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation				// gcc: Class implementation
#endif

#define MYSQL_SERVER 1
#include "sql_priv.h"
#include "unireg.h"
#include "probes_mysql.h"
#include "ha_rpl_dbus.h"
#include "sql_class.h" 
#include "rpl_mi.h"
#include "rpl_rli.h"
#include "slave.h"
#include "dbus_tcp_client.h"
#include <my_pthread.h>
#include "key.h"


#include "dbus_config.h"
#include "dbus_state.h"
#include "dbus_slave_angel.h"

#define DBUS_RELAY_HOST "127.0.0.1"
#define DBUS_RELAY_PORT 11141

uint rpl_dbus_when = 0;
ulong rpl_dbus_debug = 0;
ulonglong rpl_dbus_last_lsn = 0;
static Dbus_tcp_client* dbus_client = 0;
static Dbus_config* dbus_config = 0;
Dbus_state* dbus_state = 0;
Dbus_slave_angel* dbus_slave_angel = 0;
static  handlerton *rpl_dbus_hton = 0;

#ifndef EMBEDDED_LIBRARY
static int init_client_from_config(Dbus_tcp_client* cli, Dbus_config* cfg);
#endif

#define ENSURE_DBUS_CONFIG_INITED(err_code)   if (!dbus_config)\
  {\
    sql_print_error("D-BUS configuration not initialized");\
    return err_code;\
  }\
  if (!dbus_config->is_inited() && dbus_config->init())\
  {\
     sql_print_error("Could not intialize D-BUS configuration");\
     return err_code;\
  }
  
#ifndef EMBEDDED_LIBRARY
 
static int init_client_from_config(Dbus_tcp_client* cli, 
  Dbus_config* cfg);

static ulonglong get_lsn(Master_info* mi);
#endif

static handler *rpl_dbus_create_handler(handlerton *hton,
                                         TABLE_SHARE *table,
                                         MEM_ROOT *mem_root)
{
  return new (mem_root) ha_rpl_dbus(hton, table);
}

static int dbus_prepare_to_notify(THD* thd)
{
#ifndef EMBEDDED_LIBRARY
  if (!dbus_client)
  {
    sql_print_error("D-BUS client not initialized");
    return 1;
  }
  
  ENSURE_DBUS_CONFIG_INITED(1)
  dbus_client->set_server_id(thd->server_id);
  
  if (rpl_dbus_request_config_reload)
  {
    dbus_client->reinit();
    dbus_client->inited_from_config = 0;
    dbus_config->reinit();
    sql_print_information("Need reload, port=%d", 
      dbus_config->get_port());
    rpl_dbus_request_config_reload = 0;  
  }
  
  if (!dbus_client->inited_from_config)
  {
     if (init_client_from_config(dbus_client,dbus_config))
       return 1;
  }
  
  dbus_client->set_last_lsn(dbus_state->get_lsn());
#endif  
  return 0;
}

static int rpl_dbus_commit(handlerton *hton, THD*  thd,
        bool all)
{
#ifndef EMBEDDED_LIBRARY
  if (all)
  {
    int prepare_res = dbus_prepare_to_notify(thd);
    
    if (prepare_res)
      return prepare_res;
      
    Dbus_event e;
    e.opcode = 1;
    e.lsn = get_lsn(active_mi);
    e.ns_ts = (ulonglong)rpl_dbus_when*1000000000LL;
    e.src_id = END_OF_WINDOW_SRC_ID;
    
    const char* part_id;
    const char* real_db_name;
    uint real_db_len;
    
    if (!thd->db || dbus_parse_db_name(thd->db, thd->db_length,
      &real_db_name, &real_db_len, &part_id))
    {
      sql_print_error("RPL_DBUS: Will not send end of window for "
        " commit in database %s", thd->db ? thd->db : "NULL");
      return 0;   
    }
      
    e.l_part_id = e.phys_part_id = atoi(part_id);
    e.schema_id = 0;
    e.binlog_id = thd->master_group_id;
    e.key = "";
    e.key_len = 0;
    e.val = "";
    e.val_len = 0;
    
    if (rpl_dbus_debug)
    {
      e.print("Sending end of window event:");
    }
    
    if (dbus_client->store_and_send(dbus_config, &e))
       return 1;
  }
#endif  
  return 0;
}        


/* Static declarations for shared structures */

static mysql_mutex_t rpl_dbus_mutex;
static HASH rpl_dbus_open_tables;

static st_rpl_dbus_share *get_share(const char *table_name);
static void free_share(st_rpl_dbus_share *share);

#ifndef EMBEDDED_LIBRARY
static ulonglong get_lsn(Master_info* mi);
#endif

static int show_rpl_dbus_vars(THD *thd, SHOW_VAR *var, char *buff);

ulong rpl_dbus_request_config_reload = 0;

/*****************************************************************************
** RPL_DBUS tables
*****************************************************************************/

ha_rpl_dbus::ha_rpl_dbus(handlerton *hton,
                           TABLE_SHARE *table_arg)
  :handler(hton, table_arg),ts_field(0),row_read(0)
{
  buffer.set((char*)byte_buffer, IO_SIZE, &my_charset_bin);
}


static const char *ha_rpl_dbus_exts[] = {
  NullS
};

const char **ha_rpl_dbus::bas_ext() const
{
  return ha_rpl_dbus_exts;
}

int ha_rpl_dbus::open(const char *name, int mode, uint test_if_locked)
{
  DBUG_ENTER("ha_rpl_dbus::open");

  if (!(share= get_share(name)) || 
      !(share->save_record = (uchar*)my_malloc(table->s->reclength, 
                                MYF(MY_WME)))
     )
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  
  thr_lock_data_init(&share->lock, &lock, NULL);
  
  Field* f = 0, **f_ptr;
  for (f_ptr = table->field; f_ptr < table->field + table->s->fields;
    f_ptr++)  
  {
    f = *f_ptr;
    if (my_strcasecmp(system_charset_info, 
      f->field_name,"timestamp") == 0)
    {
      ts_field = f_ptr;  
      break;
    }  
  }

  
  DBUG_RETURN(0);
}

int ha_rpl_dbus::close(void)
{
  DBUG_ENTER("ha_rpl_dbus::close");
  free_share(share);
  DBUG_RETURN(0);
}

int ha_rpl_dbus::create(const char *name, TABLE *table_arg,
                         HA_CREATE_INFO *create_info)
{
  DBUG_ENTER("ha_rpl_dbus::create");
  DBUG_RETURN(0);
}

/*
  Intended to support partitioning.
  Allows a particular partition to be truncated.
*/
int ha_rpl_dbus::truncate()
{
  DBUG_ENTER("ha_rpl_dbus::truncate");
  DBUG_RETURN(0);
}

const char *ha_rpl_dbus::index_type(uint key_number)
{
  DBUG_ENTER("ha_rpl_dbus::index_type");
  DBUG_RETURN((table_share->key_info[key_number].flags & HA_FULLTEXT) ? 
              "FULLTEXT" :
              (table_share->key_info[key_number].flags & HA_SPATIAL) ?
              "SPATIAL" :
              (table_share->key_info[key_number].algorithm ==
               HA_KEY_ALG_RTREE) ? "RTREE" : "BTREE");
}

int ha_rpl_dbus::write_row(uchar * buf)
{
  if (rpl_dbus_debug)
    json_store(buf,1);
  if (table->next_number_field)
    update_auto_increment();
  return notify_dbus(1);
}

int ha_rpl_dbus::get_phys_part_id()
{
  ENSURE_DBUS_CONFIG_INITED(-1)
  
  if (share->part_id < 0)
    return dbus_config->get_int_table_param(
      table_share->db.str, table_share->db.length, 
      table_share->table_name.str, table_share->table_name.length,
      "physicalPartitionId");
      
  return share->part_id;
}

int ha_rpl_dbus::get_src_id()
{
  ENSURE_DBUS_CONFIG_INITED(-1)
  int src_id = dbus_config->get_int_table_param(
      share->real_db_name, share->real_db_len, 
      table_share->table_name.str, table_share->table_name.length,
      "srcId");
      
  if (src_id == -1)
  {
    src_id = create_new_src();
    
    if (src_id != -1)
      dbus_client->request_new_handshake();
  }  
  return src_id;      
}

int ha_rpl_dbus::create_new_src()
{
  ENSURE_DBUS_CONFIG_INITED(-1)
    
  return dbus_config->create_new_src(share);
}

int ha_rpl_dbus::get_l_part_id()
{
  ENSURE_DBUS_CONFIG_INITED(-1)
  return get_phys_part_id();
}

const char* ha_rpl_dbus::get_schema_file()
{
  ENSURE_DBUS_CONFIG_INITED("NULL")
  const char* val= dbus_config->get_str_table_param(
    table_share->db.str,table_share->db.length,
    table_share->table_name.str,table_share->table_name.length,
    "schemaFile");
    
  return val ? val : "NULL";  
}

const char* ha_rpl_dbus::get_schema_id()
{
  ENSURE_DBUS_CONFIG_INITED("NULL")
  const char* val= dbus_config->get_schema_id(
    table_share->db.str, table_share->db.length,
    table_share->table_name.str, table_share->table_name.length);
    
  return val;  
}

int ha_rpl_dbus::get_key(char** key, uint* len)
{
  KEY* pkey = table->key_info;
  KEY_PART_INFO* key_part;
  char buf[256];
  char buf1[512];
  char int_buf[8];
  ulonglong tmp_int;
  uint32 tmp_int32;
  uint16 tmp_int16;
  
  String s(buf, sizeof(buf),&my_charset_bin);
  s.length(0);
  String tmp(buf1,sizeof(buf1),&my_charset_bin);
  
  if (!pkey)
    goto err;
    
  for (key_part = pkey->key_part; 
    key_part < pkey->key_part + pkey->key_parts; key_part++)
  {
    Field* f = key_part->field;
    
    switch (f->type())
    {
      case MYSQL_TYPE_VARCHAR:
        if (!f->val_str(&tmp))
        {
          s.append((char)0);
        }
        else
        {
          s.append((char)tmp.length());
          s.append(tmp);        
        }  
        break;
      case MYSQL_TYPE_LONGLONG:
        tmp_int = (ulonglong)f->val_int();
        int8store(int_buf,tmp_int);
        s.append(int_buf,8);
        break;
      case MYSQL_TYPE_LONG:
        tmp_int32 = (uint32)f->val_int();
        int4store(int_buf,tmp_int32);
        s.append(int_buf,4);
        break;
      case MYSQL_TYPE_INT24:
        tmp_int32 = (uint32)f->val_int();
        int3store(int_buf,tmp_int32);
        s.append(int_buf,3);
        break;
      case MYSQL_TYPE_SHORT:
        tmp_int16 = (uint16)f->val_int();
        int2store(int_buf,tmp_int16);
        s.append(int_buf,2);
        break;
      case MYSQL_TYPE_TINY:
        s.append((uchar)f->val_int());
        break;
      default:
        goto err;    
    }
  }
  
  if (!s.length() || !(*key = (char*)sql_memdup(s.ptr(),s.length())))
    goto err;
    
  *len = s.length();  
  return 0;    
err:  
  *key = (char*)"";
  *len = 0;
  return 1;
}

int ha_rpl_dbus::get_val(char** val, uint* len)
{
  Field* f = 0, **f_ptr;
  Field_blob* fb;
  uchar* tmp;
  int res = 1;
  
  for (f_ptr = table->field; f_ptr < table->field + table->s->fields;
    f_ptr++)  
  {
    f = *f_ptr;
    if (f->type() == MYSQL_TYPE_BLOB)
      break;
  }
  
  if (!f || f->is_null()) 
    goto err;
  
  fb = (Field_blob*)f;
    
  if (!(*len = fb->get_length()))
  {
    res = 0;
    goto err;
  }
    
  fb->get_ptr((uchar**)&tmp);
  
  if (!(*val = (char*)sql_memdup(tmp,*len)))
    goto err;
    
  return 0;
err:
  *val = (char*)"";
  *len = 0;
  return res;  
}

ulonglong ha_rpl_dbus::get_ns_ts()
{
  ulonglong res = (ulonglong)rpl_dbus_when*1000000000LL;
  Field* f;
  
  if (!ts_field || !(f = *ts_field))
    return res;
  
  ulonglong tmp = (ulonglong)f->val_int();  
  
  if (f->is_null())
    return res;
    
  return tmp * 1000000LL;  
} 

int ha_rpl_dbus::notify_dbus(int opcode)
{
#ifndef EMBEDDED_LIBRARY
  if (rpl_dbus_debug)
  {
    fwrite(buffer.ptr(),buffer.length(),1,stderr);
    fprintf(stderr,"\n");
    fflush(stderr);
  }  
  
  THD* thd = ha_thd();
  
  thd->set_db(table_share->db.str, table_share->db.length);
  int prepare_res = dbus_prepare_to_notify(thd);
  
  if (prepare_res)
    return prepare_res;

  Dbus_event e;
  
  e.opcode = opcode;
  e.lsn = get_lsn(active_mi);
  e.ns_ts = get_ns_ts();
  
  /* 
   * we need to get src_id befor part_id so it will make it to 
   * the hash for the new database
   */
  e.src_id = get_src_id();
  e.phys_part_id = get_phys_part_id();
  e.l_part_id = get_l_part_id();
  e.schema_id = get_schema_id();
  e.binlog_id = thd->master_group_id;
  
  if (get_key((char**)&e.key,&e.key_len))
  {
    sql_print_error("D-BUS: Error reading key in event");
    return 1;
  }
  
  if (get_val((char**)&e.val,&e.val_len))
  {
    sql_print_error("D-BUS: Error reading value in event");
    return 1;
  }
  
  if (rpl_dbus_debug)
  {
    e.print("Sending event:");
  }

  if (dbus_client->store_and_send(dbus_config, &e))
    return 1;
#endif    
  return 0;
}

int ha_rpl_dbus::update_row(const uchar *old_data, uchar *new_data)
{
  DBUG_ENTER("ha_rpl_dbus::update_row");
  
  if (rpl_dbus_debug)
    json_store(new_data,1);
    
  return notify_dbus(1);
}

int ha_rpl_dbus::delete_row(const uchar *buf)
{
  DBUG_ENTER("ha_rpl_dbus::delete_row");
  
  if (rpl_dbus_debug)
    json_store(buf,2);
    
  return notify_dbus(2);
}

int ha_rpl_dbus::rnd_init(bool scan)
{
  DBUG_ENTER("ha_rpl_dbus::rnd_init");
  DBUG_RETURN(0);
}


int ha_rpl_dbus::rnd_next(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_rpl_dbus::rnd_next");
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str,
                       TRUE);
  THD *thd= ha_thd();
  if (thd->system_thread == SYSTEM_THREAD_SLAVE_SQL && thd->query() == NULL)
    rc= 0;
  else
    rc= HA_ERR_END_OF_FILE;
  MYSQL_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


int ha_rpl_dbus::rnd_pos(uchar * buf, uchar *pos)
{
  DBUG_ENTER("ha_rpl_dbus::rnd_pos");
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str,
                       FALSE);
  DBUG_ASSERT(0);
  MYSQL_READ_ROW_DONE(0);
  DBUG_RETURN(0);
}


void ha_rpl_dbus::position(const uchar *record)
{
  DBUG_ENTER("ha_rpl_dbus::position");
  DBUG_ASSERT(0);
  DBUG_VOID_RETURN;
}


int ha_rpl_dbus::info(uint flag)
{
  DBUG_ENTER("ha_rpl_dbus::info");

  bzero((char*) &stats, sizeof(stats));
  if (flag & HA_STATUS_AUTO)
    stats.auto_increment_value= 1;
  DBUG_RETURN(0);
}

int ha_rpl_dbus::external_lock(THD *thd, int lock_type)
{
  trans_register_ha(thd,TRUE,rpl_dbus_hton);
  return 0;  
}


THR_LOCK_DATA **ha_rpl_dbus::store_lock(THD *thd,
                                         THR_LOCK_DATA **to,
                                         enum thr_lock_type lock_type)
{
  DBUG_ENTER("ha_rpl_dbus::store_lock");
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
  {
    /*
      Here is where we get into the guts of a row level lock.
      If TL_UNLOCK is set
      If we are not doing a LOCK TABLE or DISCARD/IMPORT
      TABLESPACE, then allow multiple writers
    */

    if ((lock_type >= TL_WRITE_CONCURRENT_INSERT &&
         lock_type <= TL_WRITE) && !thd_in_lock_tables(thd)
        && !thd_tablespace_op(thd))
      lock_type = TL_WRITE_ALLOW_WRITE;

    /*
      In queries of type INSERT INTO t1 SELECT ... FROM t2 ...
      MySQL would use the lock TL_READ_NO_INSERT on t2, and that
      would conflict with TL_WRITE_ALLOW_WRITE, blocking all inserts
      to t2. Convert the lock to a normal read lock to allow
      concurrent inserts to t2.
    */

    if (lock_type == TL_READ_NO_INSERT && !thd_in_lock_tables(thd))
      lock_type = TL_READ;

    lock.type= lock_type;
  }
  *to++= &lock;
  DBUG_RETURN(to);
}


int ha_rpl_dbus::index_read_map(uchar * buf, const uchar * key,
                                 key_part_map keypart_map,
                             enum ha_rkey_function find_flag)
{
  int rc;
  DBUG_ENTER("ha_rpl_dbus::index_read");
  rc = mark_row_read();
  DBUG_RETURN(rc);
}

int ha_rpl_dbus::mark_row_read()
{
  if (row_read)
    return HA_ERR_END_OF_FILE;
  
  row_read = 1;  
  return 0;
}

int ha_rpl_dbus::index_init(uint idx, bool sorted)
{
 row_read = 0;
 active_index= idx; 
 return 0;
} 

int ha_rpl_dbus::index_read_idx_map(uchar * buf, uint idx, const uchar * key,
                                 key_part_map keypart_map,
                                 enum ha_rkey_function find_flag)
{
  int rc;
  DBUG_ENTER("ha_rpl_dbus::index_read_idx");
  rc = mark_row_read();
  DBUG_RETURN(rc);
}


int ha_rpl_dbus::index_read_last_map(uchar * buf, const uchar * key,
                                      key_part_map keypart_map)
{
  int rc;
  DBUG_ENTER("ha_rpl_dbus::index_read_last");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  THD *thd= ha_thd();
  if (thd->system_thread == SYSTEM_THREAD_SLAVE_SQL && thd->query() == NULL)
    rc= 0;
  else
    rc= HA_ERR_END_OF_FILE;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


int ha_rpl_dbus::index_next(uchar * buf)
{
  int rc;
  DBUG_ENTER("ha_rpl_dbus::index_next");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_END_OF_FILE;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


int ha_rpl_dbus::index_prev(uchar * buf)
{
  int rc;
  DBUG_ENTER("ha_rpl_dbus::index_prev");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_END_OF_FILE;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


int ha_rpl_dbus::index_first(uchar * buf)
{
  int rc;
  DBUG_ENTER("ha_rpl_dbus::index_first");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_END_OF_FILE;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
  DBUG_RETURN(HA_ERR_END_OF_FILE);
}


int ha_rpl_dbus::index_last(uchar * buf)
{
  int rc;
  DBUG_ENTER("ha_rpl_dbus::index_last");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_END_OF_FILE;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}

uint ha_rpl_dbus::json_store(const uchar* buf, uint opcode)
{

#ifndef EMBEDDED_LIBRARY
  char attribute_buffer[1024];
  bool need_record_move = (buf != table->record[0]);
  String attribute(attribute_buffer, sizeof(attribute_buffer),
                   &my_charset_bin);
  char str_buf[22];
  
  buffer.length(0);                 
  buffer.append('{');     

  buffer.append("\"opcode\":");
  buffer.qs_append(opcode);
  buffer.append(',');
  buffer.append("\"sequence\":");
  buffer.append(llstr(get_lsn(active_mi),str_buf));

  buffer.append(',');
  buffer.append("\"timestampInNanos\":");
  buffer.append(llstr(get_ns_ts(),str_buf));
  buffer.append(',');
  buffer.append("\"phsyicalPartitionId\":");
  buffer.qs_append(get_phys_part_id());
  
  buffer.append(',');
  buffer.append("\"logicalPartitionId\":");
  buffer.qs_append(get_l_part_id());
  
  buffer.append(',');
  buffer.append("\"srcId\":");
  buffer.qs_append(get_src_id());
  
  buffer.append(',');
  buffer.append("\"schemaFile\":");
  buffer.append(get_schema_file());

  buffer.append(',');
  
  if (need_record_move)
  {
    memcpy(share->save_record,table->record[0],table->s->reclength);
    memcpy(table->record[0],buf,table->s->reclength);
  }          
  for (Field **field=table->field ; *field ; field++)
  {
    const char *ptr;
    const char *end_ptr;
    const bool was_null= (*field)->is_null();

    buffer.append('"');
    buffer.append((*field)->field_name);
    buffer.append('"');
    buffer.append(':');

    (*field)->val_str(&attribute,&attribute);

    if (was_null)
      (*field)->set_null();

    if ((*field)->str_needs_quotes())
    {
      ptr= attribute.ptr();
      end_ptr= attribute.length() + ptr;

      buffer.append('"');

      for (; ptr < end_ptr; ptr++)
      {
        if (*ptr == '"')
        {
          buffer.append('\\');
          buffer.append('"');
        }
        else if (*ptr == '\r')
        {
          buffer.append('\\');
          buffer.append('r');
        }
        else if (*ptr == '\\')
        {
          buffer.append('\\');
          buffer.append('\\');
        }
        else if (*ptr == '\n')
        {
          buffer.append('\\');
          buffer.append('n');
        }
        else
          buffer.append(*ptr);
      }
      buffer.append('"');
    }
    else
    {
      buffer.append(attribute);
    }

    if (field[1])
      buffer.append(',');
  }

  if (need_record_move)
    memcpy(table->record[0],share->save_record,table->s->reclength);
  buffer.append('}');                 
  return buffer.length();
#else  
  return 0;
#endif
}

void ha_rpl_dbus::print_error(int error, myf errflag)
{
  sql_print_error("Storage engine error %d, flag: %d", error,
    errflag);
}

void ha_rpl_dbus::print_row(const uchar* buf, const char* msg)
{
  fprintf(stderr, msg);                   
  json_store(buf,0);
  fwrite(buffer.ptr(), buffer.length(), 1,stderr);
  fputc('\n',stderr);
  fflush(stderr);
}


static st_rpl_dbus_share *get_share(const char *table_name)
{
  st_rpl_dbus_share *share;
  uint length;

  length= (uint) strlen(table_name);
  mysql_mutex_lock(&rpl_dbus_mutex);
    
  if (!(share= (st_rpl_dbus_share*)
        my_hash_search(&rpl_dbus_open_tables,
                       (uchar*) table_name, length)))
  {
    if (!(share= (st_rpl_dbus_share*) my_malloc(sizeof(st_rpl_dbus_share) +
                                                 length + 1,
                                                 MYF(MY_WME | MY_ZEROFILL))))
      goto error;

    share->table_name_length= length;
    share->table_name = (char*)share + sizeof(st_rpl_dbus_share);
    strmov(share->table_name, table_name);

    // block to limited the scope of variables
    {      
      const char* db_name = share->table_name;
      
      if (*db_name == '.' && db_name[1] == '/')
        db_name += 2;
        
      const char* tmp = strchr(db_name,'/');
      const char* part_id;
      uint db_len;
      
      if (!tmp)
      {
        my_free(share);
        share = NULL;
        goto error;
      }
      
      db_len = tmp - db_name;
        
      if (dbus_parse_db_name(db_name, db_len,
        (const char**)&share->real_db_name,
        &share->real_db_len, &part_id))
        share->part_id = -1;
      else
        share->part_id = atoi(part_id);            
        
      share->table_name_no_db = (char*)++tmp;
      share->table_name_no_db_len = strlen(tmp);  
    }
    
    if (my_hash_insert(&rpl_dbus_open_tables, (uchar*) share))
    {
      my_free(share);
      share= NULL;
      goto error;
    }
    
    thr_lock_init(&share->lock);
  }
  share->use_count++;
  share->save_record = 0;
error:
  mysql_mutex_unlock(&rpl_dbus_mutex);
  return share;
}

static void free_share(st_rpl_dbus_share *share)
{
  mysql_mutex_lock(&rpl_dbus_mutex);
  if (share->save_record)
  {
    my_free(share->save_record);
    share->save_record = 0;
  }
  if (!--share->use_count)
    my_hash_delete(&rpl_dbus_open_tables, (uchar*) share);
  mysql_mutex_unlock(&rpl_dbus_mutex);
}

static void rpl_dbus_free_key(st_rpl_dbus_share *share)
{
  thr_lock_delete(&share->lock);
  my_free(share);
}

static uchar* rpl_dbus_get_key(st_rpl_dbus_share *share, size_t *length,
                                my_bool not_used __attribute__((unused)))
{
  *length= share->table_name_length;
  return (uchar*) share->table_name;
}

#ifndef EMBEDDED_LIBRARY
static ulonglong get_lsn(Master_info* mi)
{
  ulonglong log_num = 0;
  ulonglong res = 0;
  char* p;
  
  mysql_mutex_lock(&mi->rli.data_lock);
  
  if (!mi->rli.group_master_log_name)
    goto err;
  
  p = strrchr(mi->rli.group_master_log_name,'.');
  
  if (!p)
    goto err;
  
  p++;
  
  while (*p)
  {
    log_num = (*p - '0') + 10 * log_num;
    p++;
  }  
  
  res = (log_num << 32) + mi->rli.group_master_log_pos;
err:
  mysql_mutex_unlock(&mi->rli.data_lock);
  return res;  
}

#endif

#define INIT_INT_CFG(v) { int tmp = cfg->get_##v(); \
  if (tmp >=0) cli->set_##v((uint)tmp); }

#define INIT_STR_CFG(v) { const char* tmp = cfg->get_##v(); \
  if (tmp) cli->set_##v(tmp); }

#ifndef EMBEDDED_LIBRARY

static int init_client_from_config(Dbus_tcp_client* cli, 
  Dbus_config* cfg)
{
  INIT_INT_CFG(port)
  INIT_INT_CFG(connect_timeout)
  INIT_INT_CFG(read_timeout)
  INIT_INT_CFG(write_timeout)
  INIT_INT_CFG(connect_retry_wait)
  INIT_STR_CFG(host)
  cli->set_num_sources(cfg->get_num_sources());
  
  if (!cli->host)
  {
    cli->host = DBUS_RELAY_HOST;
  }
  
  if (!cli->port)
  {
    cli->port = DBUS_RELAY_PORT;
  }
  
  cli->inited_from_config = 1;
  return 0;
}  
#endif

#ifdef HAVE_PSI_INTERFACE
static PSI_mutex_key key_mutex_rpl_dbus;
PSI_mutex_key  key_mutex_dbus_slave_angel;
PSI_thread_key key_thread_dbus_slave_angel;
PSI_cond_key key_cond_dbus_slave_angel;

static PSI_thread_info all_dbus_threads[]=
{
  { &key_thread_dbus_slave_angel, "rpl_dbus", PSI_FLAG_GLOBAL},
};

static PSI_cond_info all_dbus_conds[]=
{
  { &key_cond_dbus_slave_angel, "rpl_dbus", PSI_FLAG_GLOBAL},
};


static PSI_mutex_info all_dbus_mutexes[]=
{
  { &key_mutex_rpl_dbus, "rpl_dbus", PSI_FLAG_GLOBAL},
  { &key_mutex_dbus_slave_angel, "rpl_dbus", PSI_FLAG_GLOBAL}
};

void init_rpl_dbus_psi_keys()
{
  const char* category= "rpl_dbus";
  int count;

  if (PSI_server == NULL)
    return;

  count= array_elements(all_dbus_mutexes);
  PSI_server->register_mutex(category, all_dbus_mutexes, count);
  count = array_elements(all_dbus_threads);
  PSI_server->register_thread(category, all_dbus_threads, count);
  count = array_elements(all_dbus_conds);
  PSI_server->register_cond(category, all_dbus_conds, count);
}
#endif

static int rpl_dbus_rollback(handlerton* ht __attribute__((unused)), 
  THD* thd __attribute__((unused)), 
  bool all __attribute__((unused)))
{
  return 0;
}

static int rpl_dbus_init(void *p)
{

#ifdef HAVE_PSI_INTERFACE
  init_rpl_dbus_psi_keys();
#endif

  rpl_dbus_hton= (handlerton *)p;
  rpl_dbus_hton->state= SHOW_OPTION_YES;
  rpl_dbus_hton->db_type= DB_TYPE_RPL_DBUS;
  rpl_dbus_hton->create= rpl_dbus_create_handler;
  rpl_dbus_hton->commit = rpl_dbus_commit;
  rpl_dbus_hton->rollback = rpl_dbus_rollback;
  rpl_dbus_hton->flags= HTON_CAN_RECREATE;

  mysql_mutex_init(key_mutex_rpl_dbus,
                   &rpl_dbus_mutex, MY_MUTEX_INIT_FAST);
  (void) my_hash_init(&rpl_dbus_open_tables, system_charset_info,32,0,0,
                      (my_hash_get_key) rpl_dbus_get_key,
                      (my_hash_free_key) rpl_dbus_free_key, 0);

  dbus_config = new Dbus_config();
  
  if (!dbus_config)
    return 1;
  
  dbus_client = new Dbus_tcp_client(0,0);
  
  if (!dbus_client)
    return 1;
    
  dbus_state = new Dbus_state();
  
  if (!dbus_state || dbus_state->init())
    return 1;
  
  dbus_slave_angel = new Dbus_slave_angel();
  
  if (!dbus_slave_angel || !dbus_slave_angel->is_inited() ||
    dbus_slave_angel->start())  
      return 1;
  return 0;
}

static int rpl_dbus_fini(void *p)
{
  dbus_slave_angel->terminate();
  my_hash_free(&rpl_dbus_open_tables);
  mysql_mutex_destroy(&rpl_dbus_mutex);
  delete dbus_client;
  delete dbus_config;
  delete dbus_state;
  delete dbus_slave_angel;
  dbus_client = 0;
  dbus_config = 0;
  dbus_state = 0;
  dbus_slave_angel = 0;
  return 0;
}

static MYSQL_SYSVAR_ULONG(request_config_reload,
  rpl_dbus_request_config_reload,PLUGIN_VAR_OPCMDARG,
  "Set to 1 to request configuration reload",
  NULL, NULL, 0, 0, 1, 0);
  
static MYSQL_SYSVAR_ULONG(debug,
  rpl_dbus_debug,PLUGIN_VAR_OPCMDARG,
  "Set to 1 to enable debugging output",
  NULL, NULL, 0, 0, 1, 0);
  
static struct st_mysql_sys_var* rpl_dbus_sys_vars[] =
{
  MYSQL_SYSVAR(request_config_reload),
  MYSQL_SYSVAR(debug),
  NULL
};



static SHOW_VAR rpl_dbus_status_vars[] =
{
  {"last_lsn", (char*)&rpl_dbus_last_lsn,SHOW_LONGLONG},
  {NullS, NullS, SHOW_LONG}
};

static SHOW_VAR rpl_dbus_status_vars_exp[] =
{
  {"RPL_DBUS", (char*)&show_rpl_dbus_vars,SHOW_FUNC},
  {NullS, NullS, SHOW_LONG}
};


struct st_mysql_storage_engine rpl_dbus_storage_engine=
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

static int show_rpl_dbus_vars(THD *thd, SHOW_VAR *var, char *buff)
{
  rpl_dbus_last_lsn = 0;
  if (dbus_state)
    rpl_dbus_last_lsn = dbus_state->get_lsn();
  var->type= SHOW_ARRAY;
  var->value= (char *) &rpl_dbus_status_vars;
  return 0;
}


mysql_declare_plugin(rpl_dbus)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &rpl_dbus_storage_engine,
  "RPL_DBUS",
  "Linkedin",
  "Replication to Databus",
  PLUGIN_LICENSE_GPL,
  rpl_dbus_init, /* Plugin Init */
  rpl_dbus_fini, /* Plugin Deinit */
  0x0100 /* 1.0 */,
  rpl_dbus_status_vars_exp,      /* status variables      */
  rpl_dbus_sys_vars,                       /* system variables                */
  NULL                        /* config options                  */
}
mysql_declare_plugin_end;
