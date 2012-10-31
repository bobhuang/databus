#define MYSQL_SERVER 1
#include "dbus_config.h"
#include "mysqld.h"
#include "log.h"
#include "sql_class.h"
#include "records.h"
#include "sql_base.h"
#include "m_ctype.h"
#include "my_md5.h"

#include "ha_rpl_dbus.h"

#include <ctype.h>

#define DBUS_CONFIG_DB "mysql"
#define DBUS_CONFIG_TABLE "rpl_dbus_config"

#define START_SRC_OFFSET  100

struct Dbus_hash_entry
{
  uchar* key;
  uint key_len;
  uchar* val;
  uint val_len;
  
};

Dbus_hash_entry* create_hash_entry(const uchar* key, uint key_len, 
    const uchar* val, uint val_len);
Dbus_source_entry* create_source_hash_entry(uint source_id, ulonglong lsn);

extern "C" uchar *get_dbus_hash_key(Dbus_hash_entry *e, 
                              size_t *len,
                              my_bool not_used __attribute__((unused)))
{
  *len= e->key_len;
  return (uchar*) e->key;
}

extern "C" uchar *get_dbus_hash_source_key(Dbus_source_entry *e, 
                              size_t *len,
                              my_bool not_used __attribute__((unused)))
{
  *len= sizeof(e->src_id);
  return (uchar*) &e->src_id;
}

Dbus_source_entry* create_source_hash_entry(uint src_id, ulonglong lsn)
{
  Dbus_source_entry *e = (Dbus_source_entry*)my_malloc(
    sizeof(Dbus_source_entry), MYF(MY_WME));
  
  if (!e)
    return 0;
    
  e->src_id = src_id;
  e->lsn = lsn;
  return e;
}    


Dbus_hash_entry* create_hash_entry(const uchar* key, uint key_len, 
    const uchar* val, uint val_len)
{
  Dbus_hash_entry *e = (Dbus_hash_entry*)my_malloc(
    sizeof(Dbus_hash_entry) + key_len + val_len + 1, MYF(MY_WME));
  
  if (!e)
    return 0;
    
  e->key = (uchar*)e + sizeof(Dbus_hash_entry);
  memcpy(e->key,key,key_len);
  e->key_len = key_len;   
  e->val = (uchar*)e->key + key_len;
  memcpy(e->val,val,val_len);
  e->val_len = val_len;
  e->val[val_len] = 0;
  return e;
}    


Dbus_hash::Dbus_hash():last_val(0),num_sources(0)
{
  my_hash_init(&h,&my_charset_latin1,50,0,0, 
    (my_hash_get_key)get_dbus_hash_key,my_free,0);

  (void) my_init_dynamic_array(&src_list, sizeof(Dbus_source_entry*), 64, 64);
}

Dbus_hash::~Dbus_hash()
{
  my_hash_free(&h);

  for (uint i=0; i < src_list.elements; i++)
  {
    Dbus_source_entry* fetched_entry;

    get_dynamic(&src_list,(uchar*)&fetched_entry, i);
    if (fetched_entry)
    {
      my_free(fetched_entry);
    }
  }

  delete_dynamic(&src_list);
}

uint Dbus_hash::get_num_sources()
{
  return num_sources;
}    

Dbus_source_entry* Dbus_hash::get_source(uint src_id)
{
  return 0;
}

Dbus_source_entry* Dbus_hash::first_source()
{
  return 0;
}

Dbus_source_entry* Dbus_hash::next_source()
{
  return 0;
}

int Dbus_hash::add_source(Dbus_source_entry *src)
{
  return push_dynamic(&src_list, (uchar*)&src);
}
    
int Dbus_hash::put(const char* key, const char* val, uint val_len)
{
  uint key_len = strlen(key);
  uint inc_num_sources = 0;
  
  if (!val_len)
    val_len = strlen(val);
    
  Dbus_hash_entry* e = (Dbus_hash_entry*)my_hash_search(&h,
    (const uchar*)key,key_len);
  
  if (e)
  {
    my_hash_delete(&h,(uchar*)e);
  }
  else
  {
    if (strstr(key,"srcId"))
      inc_num_sources = 1;
  }
  
  e = create_hash_entry((const uchar*)key,key_len,
                          (const uchar*)val,val_len);
  
  if (!e)
    return 1;
    
  if (my_hash_insert(&h,(const uchar*)e))
  {
    my_free(e);
    return 1;
  }
  
  last_val = e->val;
  num_sources += inc_num_sources;
  return 0;
}

const char* Dbus_hash::get(const char* key)
{
  uint key_len = strlen(key);
  Dbus_hash_entry* e =
                (Dbus_hash_entry*)my_hash_search(&h,
                (const uchar*)key,key_len);
  
  if (!e)
    return 0;
  
  return (const char*)e->val;  
}

Dbus_source_entry* Dbus_hash::srcIter::get_next_source()
{
  
  for (; idx < m_h->src_list.elements; idx++)
  {
    Dbus_source_entry* fetched_entry;

    get_dynamic(&m_h->src_list,(uchar*)&fetched_entry, idx);
    if (fetched_entry)
    {
      idx++;
      return fetched_entry;
    }
  }

  return NULL;
}

int Dbus_config::init()
{
  if (inited)
    return 0;
  
  THD* running_thd = current_thd;
  THD* thd = new THD;
  TABLE_LIST tbl;
  MEM_ROOT mem;
  READ_RECORD record_info;
  
  if (!thd)
  {
    sql_print_error("Out of memory initializing D-BUS config");
    return 1;
  }
  
  if (thd->store_globals())
  {
     sql_print_error("Error in store_globals() initializing D-BUS config");
     goto err;
  }
   
  thd->thread_stack = (char*)&thd;
  
  
  tbl.init_one_table(C_STRING_WITH_LEN(DBUS_CONFIG_DB),
                           C_STRING_WITH_LEN(DBUS_CONFIG_TABLE),
                            DBUS_CONFIG_TABLE, TL_READ);
  tbl.open_type = OT_BASE_ONLY;
  tbl.open_strategy = TABLE_LIST::OPEN_IF_EXISTS;
  
  if (open_and_lock_tables(thd, &tbl, 
       FALSE, MYSQL_LOCK_IGNORE_TIMEOUT))
  {
    sql_print_error("Error opening D-BUS config table");
    goto err;
  }     
  
  if (!tbl.table || !tbl.table->s)
  {
    sql_print_warning("Table descriptor of D-BUS config table not properly initialized possibly because it does not exist, " 
     " will live wihtout it");
    goto err;
  }
  
  if (tbl.table->s->fields != 2)
  {
    sql_print_error("Wrong number of fields in D-BUS config table");
    goto err;
  }
  
  init_sql_alloc(&mem,1024,0);
  init_read_record(&record_info,thd,tbl.table,NULL,1,0,FALSE);
  tbl.table->use_all_columns();
  
  while (!(record_info.read_record(&record_info)))
  {
    char* key, *val;
    key = get_field(&mem,tbl.table->field[0]);
    val = get_field(&mem,tbl.table->field[1]);
    
    if (key && val)
      h.put(key,val);
  }
  end_read_record(&record_info);
  free_root(&mem,MYF(0));
err:
  // consider it inited even if opening the table failed
  // rpl_dbus_request_config_reload=1 will reset inited and we
  // try again
  inited = 1;
  close_mysql_tables(thd);
  
  delete thd;  
  
  if (running_thd)
  {
    running_thd->store_globals();
  }
  else
  {
    my_pthread_setspecific_ptr(THR_THD,0);
    my_pthread_setspecific_ptr(THR_MALLOC,0);
  }  
  return !inited;  
}

int Dbus_config::alloc_vars()
{
  init_sql_alloc(&mem_root,1024,0);
  return 0;
}

int Dbus_config::free_vars()
{
  free_root(&mem_root,MYF(0));
  return 0;
}

int Dbus_config::get_int_table_param(const char* db_name, uint db_len,
     const char* table_name, uint table_name_len,
     const char* param_name)
{
  const char* val = get_str_table_param(db_name, db_len,
  table_name, table_name_len,param_name);
  
  if (!val)
    return -1;
    
  return atoi(val);  
}
     
const char* Dbus_config::get_str_table_param(const char* db_name,
     uint db_len,
     const char* table_name, uint table_name_len,
     const char* param_name)
{
  char buf[256], *p;
  uint param_name_len = strlen(param_name);
  p = strmov((char*)buf, CONFIG_PREFIX"source(");
  
  if (p + db_len + db_len + param_name_len + 4 > buf + sizeof(buf))
    return 0;
     
  memcpy(p,db_name,db_len);
  p += db_len;
  *p++ = '.';
  memcpy(p,table_name,table_name_len);
  p += table_name_len;
  *p++ = ')';
  *p++ = '.';
  strmov(p,param_name);
  return h.get(buf);
}     

const char* Dbus_config::get_schema_id(const char* db_name, uint db_len,
   const char* table_name, uint table_name_len)
{
  const char* res = get_str_table_param(db_name,
    db_len,table_name, table_name_len,"schemaId");
  
  if (!res)
  {
    const char* file = get_str_table_param(db_name, db_len,
       table_name, table_name_len, "schemaFile");
    
    if (!file)
      return 0;
      
    uchar digest[16];
    char buf[256];
    my_snprintf(buf,sizeof(buf),CONFIG_PREFIX"source(%s.%s).schemaId",
       db_name,table_name);
    MY_MD5_HASH(digest,(uchar*)file, strlen(file));
    
    if (h.put((const char*)buf,(const char*)digest,sizeof(digest)))
      return 0;
      
    res = h.get_last_val();  
  }
  
  return res;
}

int dbus_parse_db_name(const char* db_name, uint db_len, 
  const char** real_db_name, uint* real_db_len, const char** part_id)
{
  const char* p_end = db_name + db_len, *p;
  *part_id = "0";
  *real_db_len = db_len;
  *real_db_name = db_name;

  for (p = p_end; p > db_name && *p != '_'; p--);
  
  if (p != db_name && p != p_end && isdigit(*++p))
  {
    *part_id = p;
    
    for ( p -= 2; p > db_name && *p != '_'; p--);
    
    if (p != db_name)
    {
      *real_db_name = p + 1;
      *real_db_len = *part_id - p - 2;
    } 
    else
      *real_db_len = *part_id - db_name - 1; 
      
    return 0;  
  }

  return 1;
}  


int Dbus_config::create_new_src(struct st_rpl_dbus_share* share)
{
  char buf[256];
  char srcId[22];
  int srcnum = get_num_sources() + START_SRC_OFFSET;
  Dbus_source_entry *srcinfo = (Dbus_source_entry*)my_malloc(
    sizeof(Dbus_source_entry), MYF(MY_WME));

  if (!srcinfo)
    return -1;

  String s(buf,sizeof(buf),&my_charset_latin1);
  s.length(0);
  s.append(C_STRING_WITH_LEN(CONFIG_PREFIX"source("));
  
  const char* real_db_name = share->real_db_name;
  uint real_db_len = share->real_db_len;
  const char* table_name = share->table_name_no_db;
  uint table_name_len = share->table_name_no_db_len;
  
  if (real_db_len + 
      table_name_len + 2 > sizeof(srcinfo->src_name))
    return -1;
    
  memcpy(srcinfo->src_name, real_db_name, real_db_len);
  srcinfo->src_name[real_db_len] = '.';
  memcpy(srcinfo->src_name + real_db_len + 1, table_name, 
    table_name_len + 1); // table_name is 0 terminated
    
  s.append(real_db_name,real_db_len);
  s.append('.');
  s.append(table_name, table_name_len);
  s.append(C_STRING_WITH_LEN(").srcId"));

  llstr(srcnum, srcId);
 
  srcinfo->src_id = srcnum;
  srcinfo->src_len = real_db_len + table_name_len + 1;

  if (h.add_source(srcinfo) || h.put(s.c_ptr(), srcId))
    return -1;
    
  return srcnum;
}
