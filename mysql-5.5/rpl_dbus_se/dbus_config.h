#ifndef DBUS_CONFIG_H
#define DBUS_CONFIG_H

#include "hash.h"
#include "sql_const.h"
#include <string.h>

#include "log.h"

struct Dbus_source_entry
{
  uint src_id;
  ulonglong lsn;
  uint src_len;
  char src_name[256];
};

class Dbus_hash
{
  protected:
    HASH h;
    uchar* last_val;
    uint num_sources;
    DYNAMIC_ARRAY src_list;
 
  public:
    Dbus_hash();
    ~Dbus_hash(); 
    
    int put(const char* key, const char* val, uint val_len=0);
    const char* get(const char* key); 
    const char* get_last_val() { return (char*)last_val; }
    uint get_num_sources();
    Dbus_source_entry* get_source(uint src_id);
    Dbus_source_entry* first_source();
    Dbus_source_entry* next_source();
    int add_source(Dbus_source_entry* src);    

    class srcIter
    {
      private:
        uint idx;
        Dbus_hash* m_h;

      public:
        srcIter():idx(0) {}
        void set_hash(Dbus_hash *h) { m_h = h; }
        Dbus_source_entry* get_next_source(); 
    };

};

#define CONFIG_PREFIX "databus2.mysql."
#define GET_INT_CONFIG_VAR(v,config_v) int get_ ## v () \
  { \
    if (v >= 0) return v;\
    const char* val = h.get(CONFIG_PREFIX#config_v);\
    if (!val) return -1;\
    v = atoi(val);\
    return v;}

#define GET_STR_CONFIG_VAR(v,config_v) const char* get_ ## v () \
  { \
    if (v) return v;\
    const char* val = h.get(CONFIG_PREFIX#config_v);\
    if (!val) return 0;\
    v = strmake_root(&mem_root,val,strlen(val));\
    return v;}

extern ulong rpl_dbus_request_config_reload;

class Dbus_config
{
  public:
  
  protected:
    MEM_ROOT mem_root;
    bool inited;
    Dbus_hash h;
    int read_timeout;
    int write_timeout;
    int connect_timeout;
    int connect_retry_wait;
    char *host;
    int port;
    int phys_part_id;
    
        
  public:
   Dbus_config() { alloc_vars(); reset();}
   ~Dbus_config() { free_vars(); }
   
   bool is_inited() 
   {
     return  inited; 
   }
   int init();
   void reset()
   {
     read_timeout = -1;
     write_timeout = -1;
     port = -1;
     phys_part_id = -1;
     connect_timeout = -1;
     host = 0;
     connect_retry_wait = -1;
     inited = 0;
   }
   int reinit() { reset(); return init();}
   int alloc_vars();
   int free_vars();
   
   GET_INT_CONFIG_VAR(port,port)
   GET_INT_CONFIG_VAR(read_timeout,readTimeout)
   GET_INT_CONFIG_VAR(write_timeout,writeTimeout)
   GET_INT_CONFIG_VAR(connect_timeout,connectTimeout)
   GET_INT_CONFIG_VAR(connect_retry_wait,connectRetryWait)
   GET_STR_CONFIG_VAR(host,host)
   
   int get_int_table_param(const char* db_name, uint db_len, 
     const char* table_name, uint table_name_len,
     const char* param_name);
   const char* get_str_table_param(const char* db_name, uint db_len,
     const char* table_name, uint table_name_len,
     const char* param_name);
     
   const char* get_schema_id(const char* db_name,uint db_len, 
     const char* table_name, uint table_name_len); 
      
   uint get_num_sources() { return h.get_num_sources();}
   int create_new_src(struct st_rpl_dbus_share* share);
   void init_src_iter(Dbus_hash::srcIter *iter)  { iter->set_hash(&h); }
};

int dbus_parse_db_name(const char* db_name, uint db_len, 
  const char** real_db_name, uint* real_db_len, const char** part_id);

#endif
