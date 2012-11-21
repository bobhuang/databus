#include <mysql.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/time.h>
#include <string.h>
#include <unistd.h>
#include "libbench.h"

static const char* host = "localhost";
static int port = 3306;
static const char* user = "root";
static const char* pw = "";
//static const char* db = "test";
static const char* sock = 0;
static int num_dbs = 1;
static int num_tables = 1;
static int num_rounds = 0;
static int num_threads = 0;
static int num_joins = 1;
static int md5_repeat_count = 0;
static int sec_key_len = 30;
static int num_secondary_keys = 0;
static int blob_len = 65535;
static const char* data_fname = "data.txt";
static char* datadir = 0;
static char* innodb_log_dir = 0;
static unsigned long long innodb_log_size = 0;
static MYSQL* glob_mysql = 0;
static const char* table_opts = "";
static const char* target_dir = "var/tmp/xtrabackup";
static int xtrabackup_sleep = 1;
static int wait_for_log_flush = 0;
static useconds_t sleep_between_queries = 0;
static ulonglong test_time = 0;
static int pre_warm = 0;
static const char* load_ratios = 0;
static int no_xtrabackup = 0;

struct Load_ratios
{
  long selects;
  long inserts;
  long updates;
  long deletes;
  
  void init_from_string(const char* s);
};

static Load_ratios lr;

int min_key = 0, max_key = 0;

void die(const char *fmt, ...);
void parse_args(int argc,char** argv);
  
void glob_db_connect();

void usage();
void do_exit(int res);
void* run_thread(void* arg);
void start_threads();
void wait_for_threads();
void cleanup();
int timed_query(MYSQL* mysql, int with_result, ulonglong* resp_time,
  const char* query, ...);
int get_rand(unsigned int* seed, int min_r, int max_r);
int get_db(int tab_num);
void print_stats();
void print_test_info();
void print_ts();
void print_mysql_vars();
void populate_tables();
int populate_table(MYSQL* mysql, int table_num);
int populate_table_with_keys(MYSQL* mysql, int table_num);
int populate_ext_keys(MYSQL* mysql, int table_num);
void populate_file();
void glob_db_close();
void setup_db_and_tables();
void create_db(int);
void create_table(int);
void setup_secondary_key_test();
void create_table_with_keys(int num);
void create_external_keys(int num);
void mk_join_query(unsigned int* seed,
   char* buf, size_t buf_size, int table_num);
   
void mk_key_query(unsigned int* seed,
   char* buf, size_t buf_size, int table_num);
   
void mk_key_insert(unsigned int* seed,
   char* buf, size_t buf_size, int table_num);
void mk_ext_insert(unsigned int* seed,
   char* buf, size_t buf_size, int table_num);
   
void mk_key_delete(unsigned int* seed,
   char* buf, size_t buf_size, int table_num);
void mk_ext_delete(unsigned int* seed,
   char* buf, size_t buf_size, int table_num);
   
void mk_key_update(unsigned int* seed,
   char* buf, size_t buf_size, int table_num);
void mk_ext_update(unsigned int* seed,
   char* buf, size_t buf_size, int table_num);

int table_copy_exists(const char* prefix, int num);

void print_mysql_vars();
void run_var_query(const char* msg_to_print, const char* query, 
  const char** vars_to_print,
  int init_datadir);

void copy_table_files(const char* src, const char* dst,
  int start_ind);

void import_tablespacef(const char* tbl,...);

const char* mysql_vars[] =
{
  "innodb_file_per_table",
  "innodb_flush_log_at_trx_commit",
  "innodb_buffer_pool_size",
  "innodb_flush_method",
  "innodb_doublewrite",
  "innodb_thread_concurrency",
  "innodb_log_file_size",
  "innodb_tablespace_extra_extend",
  "innodb_flush_neighbor_pages",
  "innodb_io_capacity",
  "innodb_adaptive_checkpoint",
  "innodb_checkpoint_age_target",
  "innodb_adaptive_flushing",
  NULL
}; 

const char* mysql_status_vars[] =
{
  "Innodb_data_fsyncs",
  "Innodb_os_log_fsyncs",
  NULL
}; 


struct Bench_thread
{
  int thread_num;
  int running;
  pthread_mutex_t lock;
  pthread_cond_t cond;
  Response_stats insert_stats, select_stats, 
    update_stats, delete_stats, 
    select_sec_key_stats, select_ext_key_stats, 
    insert_sec_key_stats, insert_ext_key_stats, 
    update_sec_key_stats, update_ext_key_stats, 
    delete_sec_key_stats, delete_ext_key_stats 
    ;
  
  Bench_thread()
  {  
    init();
  }
  ~Bench_thread()
  {
    clean();
  }
  
  int clean()
  {
    pthread_mutex_destroy(&lock);
    pthread_cond_destroy(&cond);
    return 0;
  }
  
  int init()
  {
    running = 0;
    pthread_cond_init(&cond,0);
    pthread_mutex_init(&lock,0);
    return 0;
  }
  
  void signal_exit()
  {
    pthread_mutex_lock(&lock);
    running = 0;
    pthread_cond_broadcast(&cond);
    pthread_mutex_unlock(&lock);
  }
  
  void wait_for_exit()
  {
    pthread_mutex_lock(&lock);
    
    while (running)
      pthread_cond_wait(&cond,&lock);
    pthread_mutex_unlock(&lock);
  }
  
  void update_g_stats();
  
};

Bench_thread* threads = 0;
__thread Bench_thread* cur_thread = 0;
Response_stats g_insert_stats,
  g_select_stats,g_delete_stats,g_update_stats,
  g_select_sec_key_stats, g_select_ext_key_stats,
  g_insert_sec_key_stats, g_insert_ext_key_stats,
  g_update_sec_key_stats, g_update_ext_key_stats,
  g_delete_sec_key_stats, g_delete_ext_key_stats
  ;

static struct option opts[] =
{
  {"host",required_argument, 0, 'h'},
  {"user",required_argument, 0, 'u'},
  {"password",required_argument, 0, 'p'},
  {"num-dbs",required_argument, 0, 'D'},
  {"socket",required_argument, 0, 'S'},
  {"port",required_argument, 0, 'P'},
  {"num-threads",required_argument, 0, 'T'},
  {"num-tables",required_argument, 0, 't'},
  {"num-rounds",required_argument, 0, 'r'},
  {"num-secondary-keys",required_argument, 0, 's'},
  {"num-joins",required_argument, 0, 'j'},
  {"min-key",required_argument, 0, 'k'},
  {"max-key",required_argument, 0, 'K'},
  {"blob-len",required_argument, 0, 'b'},
  {"table-opts",required_argument, 0, 'o'},
  {"target-dir",required_argument, 0, 'd'},
  {"sleep-between-queries",required_argument, 0, 'U'},
  {"pre-warm",required_argument,0,'w'},
  {"load-ratios",required_argument,0,'l'},
  {"no-xtrabackup",no_argument,0,'x'},
  {0,0,0,0}
};

void Bench_thread::update_g_stats()
{
  g_insert_stats.add(&insert_stats);
  g_select_stats.add(&select_stats);
  g_delete_stats.add(&delete_stats);
  g_update_stats.add(&update_stats);
  
  g_select_sec_key_stats.add(&select_sec_key_stats);
  g_select_ext_key_stats.add(&select_ext_key_stats);
  
  g_update_sec_key_stats.add(&update_sec_key_stats);
  g_update_ext_key_stats.add(&update_ext_key_stats);
  
  g_insert_sec_key_stats.add(&insert_sec_key_stats);
  g_insert_ext_key_stats.add(&insert_ext_key_stats);
  
  g_delete_sec_key_stats.add(&delete_sec_key_stats);
  g_delete_ext_key_stats.add(&delete_ext_key_stats);
}

int get_db(int tab)
{
  return ((tab-1)%num_dbs)+1;
}

void setup_dbs_and_tables()
{
  for (int i = 1; i <= num_dbs; i++)
  {
    create_db(i);
  }

  for (int i = 1; i <= num_tables; i++)
  {
    create_table(i);
  }
}

void create_db(int dbnum)
{
  safe_queryf(glob_mysql,"drop database if exists db_%d ", dbnum);
  safe_queryf(glob_mysql,"create database db_%d ", dbnum);
}

void create_table(int tabnum)
{
  safe_queryf(glob_mysql,
       "create table db_%d.t_%d(k bigint not null primary key,"
                 "val blob) engine=innodb %s", get_db(tabnum), 
                 tabnum, table_opts);
}

void create_table_with_keys(int tab)
{
  int i;
  char buf[2048];
  int pos;
    
  pos = snprintf(buf, sizeof(buf), 
		 "create table db_%d.tk_%d(k bigint not null primary key,"
		 "val blob", get_db(tab), tab);
  
  for (i = 0; i < num_secondary_keys; i++)
  {
    pos += snprintf(buf + pos, sizeof(buf) - pos, 
		    ", k%d varchar(%d)", i + 1, sec_key_len);
  }
  
  for (i = 0; i < num_secondary_keys; i++)
  {
    pos += snprintf(buf + pos, sizeof(buf) - pos, 
      ", key(k%d,k)", i + 1);
  }
  
  pos += snprintf(buf + pos, sizeof(buf) - pos, 
    ") engine=innodb %s", table_opts); 
  safe_queryf(glob_mysql,buf);
}

void create_external_keys(int tab)
{
  int i;
  
  for (i = 1; i <= num_secondary_keys; i++)
  {
     safe_queryf(glob_mysql, "create table db_%d.t_ext_k_%d_%d("
     "k bigint not null, sec_k varchar(%d),"
		 "primary key(sec_k,k),unique key(k)) %s", get_db(tab), tab, i, 
     sec_key_len,
     table_opts); 
  }
}



void setup_secondary_key_test()
{
  if (!num_secondary_keys)
    return;
    
  md5_repeat_count = (sec_key_len - 1) / 32 + 1;
    
  for (int i = 1; i <= num_tables; i++)
  {
    create_table_with_keys(i);
    create_external_keys(i);
  }  
}

void run_var_query(const char* msg_to_print, const char* query, 
  const char** vars_to_print,
  int init_datadir)
{
  MYSQL_RES* res = 0;
  int num_fields;
  MYSQL_ROW row;

  if (mysql_real_query(glob_mysql,query,strlen(query)))
  {
    warn("Error running %s: %s", query, mysql_error(glob_mysql));
    goto err;
  }
  
  if (!(res = mysql_store_result(glob_mysql)))
  {
    warn("Error in mysql_store_result(): %s", 
      mysql_error(glob_mysql));
    goto err; 
  }
  
  if ((num_fields = mysql_num_fields(res)) != 2)
  {
    warn("Wrong number of rows in %s output: expected 2, got %d",
      query, num_fields);
    goto err;  
  }
  
  printf("%s:\n", msg_to_print);
  
  while ((row = mysql_fetch_row(res)))
  {
    const char** var_name = vars_to_print;
    
    if (!row[0])
      continue;
    
    while (*var_name)
    {
      if (!strcasecmp(*var_name,row[0]))
      {
        printf("%s %s\n", *var_name, row[1]);
      }
      var_name++;
    }
    
    if (init_datadir)
    {
      if (!strcasecmp("datadir",row[0]))
      {
        datadir = strdup(row[1]);
      }
      
      if (!strcasecmp("innodb_log_group_home_dir",row[0]))
      {
        innodb_log_dir = strdup(row[1]);
      }
      
      if (!strcasecmp("innodb_log_file_size",row[0]))
      {
        innodb_log_size = strtoull(row[1],0,10);
      }
    }
  }
  
err:      
  if (res)
  {
    mysql_free_result(res);
    res = 0;
  }
}  

// initialized datadir as a side effect
void print_mysql_vars()
{
  run_var_query("MySQL configuration variables of interest",
    "show variables", mysql_vars, 1);
  run_var_query("MySQL status variables of interest" 
   " before the test",
    "show status", mysql_status_vars, 0);
}

void mk_key_query(unsigned int* seed, 
		  char* buf, size_t buf_size, int table_num)
{
  int pos = 0; 
  int i;
    
  for (i = 1; i <= num_joins; i++)
  {
    int key_num = get_rand(seed,1,num_secondary_keys);
    int key_val = get_rand(seed,min_key,max_key);
    
    if (i > 1)
    {
      pos += snprintf(buf + pos, buf_size - pos, " union ");
    }
    
    pos += snprintf(buf + pos, buf_size - pos, 
		    " select * from db_%d.tk_%d where k%d = "
		    " substring(repeat(md5(%d),%d),1,%d)", get_db(table_num), table_num,
		    key_num,key_val,md5_repeat_count,
		    sec_key_len);
  }  
  
}

void mk_key_update(unsigned int* seed,
		   char* buf, size_t buf_size, int table_num)
{
  int pos; 
  int i;
  int key_val = get_rand(seed,min_key,max_key);
  
  pos = snprintf(buf, buf_size, 
  "update db_%d.tk_%d set val = repeat('u',%d)", 
		 get_db(table_num), table_num, blob_len, key_val);
     
    
  for (i = 1; i <= num_secondary_keys; i++)
  {
    pos += snprintf(buf + pos, 
      sizeof(buf) - pos, 
    ", k%d = substring(reverse(repeat(md5(%d),%d)),1,%d)", 
     i,key_val,md5_repeat_count,
     sec_key_len);
  }  
  
  pos += snprintf(buf + pos, 
      sizeof(buf) - pos, " where k = %d", key_val);
  
}
   
void mk_ext_update(unsigned int* seed,
		   char* buf, size_t buf_size, int table_num)
{
  int pos; 
  int i;
  int key_val = get_rand(seed,min_key,max_key);
  
  pos = snprintf(buf, buf_size, "update db_%d.tk_%d tk", get_db(table_num), table_num);
  
  for (i = 1; i <= num_secondary_keys; i++)
  {
    pos += snprintf(buf + pos, buf_size - pos, 
		    ",db_%d.t_ext_k_%d_%d t%d", get_db(table_num), table_num, i, i);
  }
  
  pos += snprintf(buf + pos, buf_size - pos, 
    " set tk.val = repeat('u',%d)", 
    blob_len);
     
    
  for (i = 1; i <= num_secondary_keys; i++)
  {
    pos += snprintf(buf + pos, 
      sizeof(buf) - pos, 
    " ,t%d.sec_k = substring(reverse(repeat(md5(%d),%d)),1,%d)", 
     i,key_val,md5_repeat_count,
     sec_key_len);
  }  
  
  pos += snprintf(buf + pos, buf_size - pos, 
    " where tk.k = %d ", key_val);
    
  for (i = 1; i <= num_secondary_keys; i++)
  {
    pos += snprintf(buf + pos, 
      sizeof(buf) - pos, 
    " and tk.k = t%d.k", 
     i);
  }  
  
}   

void mk_key_delete(unsigned int* seed,
		   char* buf, size_t buf_size, int table_num)
{
  int key_val = get_rand(seed,max_key,2*max_key - min_key);
  
  snprintf(buf, buf_size, 
	   "delete from db_%d.tk_%d where k = %d", 
	   get_db(table_num), table_num, key_val);
}
   
void mk_ext_delete(unsigned int* seed,
		   char* buf, size_t buf_size, int table_num)
{
  int pos; 
  int i;
  int key_val = get_rand(seed,max_key,2*max_key-min_key);
  
  pos = snprintf(buf, buf_size, 
  "delete tk");
  
  for (i = 1; i <= num_secondary_keys; i++)
  {
    pos += snprintf(buf + pos, buf_size - pos, 
      ",t%d", i);
  }

  pos += snprintf(buf + pos, buf_size - pos, 
		  " from tk_%d tk", table_num);
  
  for (i = 1; i <= num_secondary_keys; i++)
  {
    pos += snprintf(buf + pos, buf_size - pos, 
      ",t_ext_k_%d_%d t%d", table_num, i, i);
  }
  
  pos += snprintf(buf + pos, buf_size - pos, 
    " where tk.k = %d ", key_val, 
    blob_len);
    
  for (i = 1; i <= num_secondary_keys; i++)
  {
    pos += snprintf(buf + pos, 
      sizeof(buf) - pos, 
    " and tk.k = t%d.k", 
     i);
  }  
}   


void mk_key_insert(unsigned int* seed,
		   char* buf, size_t buf_size, int table_num)
{
  int pos; 
  int i;
  int key_val = get_rand(seed,max_key, 2*max_key - min_key);
  char dup_key_buf[2048];
  int dup_key_pos; 
  
  pos = snprintf(buf, buf_size, 
		 "insert into db_%d.tk_%d values(%d, repeat('g',%d),", 
		 get_db(table_num), table_num, key_val, blob_len);
     
  dup_key_pos = snprintf(dup_key_buf,sizeof(dup_key_buf),
    " on duplicate key update val = repeat('g',%d), ", blob_len);
    
  for (i = 1; i <= num_secondary_keys; i++)
  {
    if (i > 1)
    {
      pos += snprintf(buf + pos, buf_size - pos, ",");
      dup_key_pos += snprintf(dup_key_buf + 
        dup_key_pos, sizeof(dup_key_buf) - pos, ",");
    }
    
    pos += snprintf(buf + pos, buf_size - pos, 
    " substring(repeat(md5(%d),%d),1,%d)", 
     key_val,md5_repeat_count,
     sec_key_len);
    dup_key_pos += snprintf(dup_key_buf + dup_key_pos, 
      sizeof(dup_key_buf) - dup_key_pos, 
    " k%d = substring(repeat(md5(%d),%d),1,%d)", 
     i,key_val,md5_repeat_count,
     sec_key_len);
  }  
  
  pos += snprintf(buf + pos,buf_size - pos, ") %s", dup_key_buf);
}
   
void mk_ext_insert(unsigned int* seed,
		   char* buf, size_t buf_size, int table_num)
{
  int pos; 
  int i;
  int key_val = get_rand(seed,max_key,2*max_key - min_key);
  
  pos = snprintf(buf, buf_size, 
		 "insert into db_%d.tk_%d values(%d,repeat('g',%d)",
		 get_db(table_num), table_num, key_val,blob_len);
    
  for (i = 1; i <= num_secondary_keys; i++)
  {
    pos += snprintf(buf + pos, buf_size - pos, 
      ", substring(repeat(md5(%d),%d),1,%d)",
      key_val,md5_repeat_count,
      sec_key_len);
  }
  
  pos += snprintf(buf + pos, buf_size - pos, 
    ") on duplicate key update ");
    
  for (i = 1; i <= num_secondary_keys; i++)
  {
    if (i > 1)
    {
      pos += snprintf(buf + pos, buf_size - pos, ",");
    }
    
    pos += snprintf(buf + pos, buf_size - pos, 
      "k%d = substring(repeat(md5(%d),%d),1,%d)",
      i, key_val,md5_repeat_count,
      sec_key_len);
  }
    
  for (i = 1; i <= num_secondary_keys; i++)
  {
     pos += snprintf(buf + pos, buf_size - pos, 
    "; insert into db_%d.t_ext_k_%d_%d values(%d,"
    "substring(repeat(md5(%d),%d),1,%d)) " 
    " on duplicate key update sec_k = "
    "substring(repeat(md5(%d),%d),1,%d)",
     get_db(table_num), table_num, i, key_val, key_val,md5_repeat_count,
     sec_key_len, key_val,md5_repeat_count,
     sec_key_len);
  }  
}   


void mk_join_query(unsigned int* seed, 
		   char* buf, size_t buf_size, int table_num)
{
  int pos =  0; 
  int i;
    
  for (i = 1; i <= num_joins; i++)
  {
    int key_num = get_rand(seed,1,num_secondary_keys);
    int key_val = get_rand(seed,min_key,max_key);
    
    if (i > 1)
    {
      pos += snprintf(buf + pos, buf_size - pos, " union ");
    }
    
    pos += snprintf(buf + pos, buf_size - pos, 
    "select t.* from db_%d.t_ext_k_%d_%d t%d force key(primary) " 
    " straight_join db_%d.t_%d t force key(primary)"
    "  where t.k = t%d.k  "
    " and t%d.sec_k = "
    " substring(repeat(md5(%d),%d),1,%d)", 
    get_db(table_num), table_num, key_num,i,get_db(table_num),table_num,
    i,i,key_val,md5_repeat_count,
    sec_key_len);
  }  
  
}


void glob_db_close()
{
  if (glob_mysql)
  {
    mysql_close(glob_mysql);
    glob_mysql = 0;
  }  
}


void glob_db_connect()
{
  glob_mysql = mysql_init(0);
  
  if (!glob_mysql)
    die("Out of memory");
    
  if (!mysql_real_connect(glob_mysql,host,user,pw,NULL,port,sock,
    CLIENT_MULTI_STATEMENTS))
  {
    die("Error connecting to MySQL: %s", mysql_error(glob_mysql));
  }

  safe_queryf(glob_mysql, "set global innodb_expand_import=1");
}

void copy_table_files(const char* src, const char* dst,
		      int start_ind, int end_ind)
{
  char src_fname[FILENAME_MAX], dst_fname[FILENAME_MAX];

  snprintf(src_fname,sizeof(src_fname), 
     "%s/db_1/%s.ibd", target_dir, src);
  snprintf(dst_fname,sizeof(dst_fname),"%s/db_%%d/%s", datadir, dst);
  
  if (copy_files(src_fname, dst_fname, start_ind, end_ind, ".ibd", &get_db))
  {
    die("File copy of ibd files failed");
  }
  
  snprintf(src_fname,sizeof(src_fname), 
     "%s/db_1/%s.exp", target_dir, src);
     
  if (copy_files(src_fname, dst_fname, 2, num_tables, ".exp", &get_db))
  {
    die("File copy of exp files failed");
  }

}


void print_test_info()
{
  puts("Test started  ");
  print_ts();
  fputc('\n',stdout);
  print_mysql_vars();
  printf("Threads: %d, Databases: %d, Tables: %d, Rounds: %d, Min key: %d, "
   "Max key: %d, Blob len: %d\n"
   "Secondary keys: %d Secondary key len: %d Num joins: %d\n",
   num_threads, num_dbs, num_tables, num_rounds, 
   min_key, max_key, blob_len, num_secondary_keys,
   sec_key_len, num_joins);
}

void print_ts()
{
  char buf[256];
  time_t t;
  struct tm *tmp;
  
  t = time(NULL);
  tmp = localtime(&t);
  
  if (!tmp)
    return;
    
  if (!strftime(buf,sizeof(buf),"on %Y-%m-%d at %H:%M:%S",tmp))
    return;
    
  puts(buf);    
}

int get_rand(unsigned int* seed, int min_r, int max_r)
{
  int r = rand_r(seed);
  return min_r + r % (max_r - min_r + 1); 
}

void do_exit(int res)
{
  if (cur_thread)
  {
    msg("Exiting thread %d ", cur_thread->thread_num);
    cur_thread->signal_exit();
    pthread_exit((void*)res);
  }  
  else
    exit(res);
}

int timed_query(MYSQL* mysql, int with_result, ulonglong* resp_time,
  const char* query, ...)
{
  char buf[2048];
  struct timeval tv_start, tv_end;
  int query_len;
  int status;
  va_list args;
  va_start(args,query);
  query_len = vsnprintf(buf,sizeof(buf),query,args);
  va_end(args);
  
  if (sleep_between_queries)
    usleep(sleep_between_queries);
  
  gettimeofday(&tv_start,0);
  
  if (mysql_real_query(mysql,buf,query_len))
  {
    warn("Error running query '%s': %s", buf, mysql_error(mysql));
    return 1;
  }
  
  do
  {
    MYSQL_RES* res = mysql_store_result(mysql);
    if (!res)
    {
      if (mysql_field_count(mysql)) 
      {
        warn("Error in mysql_store_result(): %s",
           mysql_error(mysql));
        return 1;
      }  
    }
    
    if (res)
      mysql_free_result(res);
    
    if ((status = mysql_next_result(mysql)) > 0)
    {
      warn("Error retrieving next result");
      return 1;
    }
  }
  while (status == 0);
  
  gettimeofday(&tv_end,0);
  
  if (resp_time)
    *resp_time = get_time_diff(&tv_start,&tv_end);
    
  return 0;
}  



void usage()
{
  die("Usage: ./multi-table-bench [arguments]");
}

void wait_for_threads()
{
  int i;
  
  for (i = 0; i < num_threads; i++)
  {
    Bench_thread* t = threads + i;
    t->wait_for_exit();
    t->update_g_stats();
  }
}

void cleanup()
{
  if (datadir)
  {
    free(datadir);
    datadir = 0;
  }
  
  if (innodb_log_dir)
  {
    free(innodb_log_dir);
    innodb_log_dir = 0;
  }
  delete[] threads; 
}


void start_threads()
{
  int i;
  threads = new Bench_thread[num_threads];
  for (i = 0; i < num_threads; i++)
  {
    pthread_t thread_id;
    threads[i].thread_num = i;
    threads[i].running = 2;
    
    if (pthread_create(&thread_id,0,run_thread,(void*)(threads+i)))
    {
      threads[i].running = 0;
      warn("Could not create thread");
    }
  }
}

void* run_thread(void* arg)
{
  Bench_thread* t = (Bench_thread*)arg;
  cur_thread = t;
  MYSQL* mysql = mysql_init(0);
  MYSQL_RES * res = 0;
  unsigned int seed = (unsigned int)(((unsigned long long)&mysql) & 0xffffffff) ;
  
  char buf[8192];
  int query_len;
  ulonglong resp_time;
  int j;
  int i;
  
  if (!mysql)
    die("Out of memory");
    
  
  if (!mysql_real_connect(mysql,host,user,pw,NULL,port,sock,
    CLIENT_MULTI_STATEMENTS))
  {
    mysql_close(mysql);
    die("Error connecting to MySQL: %s", mysql_error(mysql));
  }
  
  for (i = 0; i < num_rounds; i++)
  {
    for (j = 0; j < lr.selects; j++)
    {
      int tab_num = get_rand(&seed,1, num_tables);
      int db_num = get_db(tab_num);

      if (timed_query(mysql,1,&resp_time,
        "select * from db_%d.t_%d where k = %d",
        db_num, tab_num,
        get_rand(&seed,min_key,max_key)))
      {
        die("Error running select query: %s - exiting ", 
         mysql_error(mysql));
      }   
      t->select_stats.add_response(resp_time);
    }
    
    for (j = 0; j < lr.inserts; j++)
    {
      int tab_num = get_rand(&seed,1,num_tables);
      int db_num = get_db(tab_num);

      if (timed_query(mysql,0,&resp_time,
        "insert into db_%d.t_%d values(%d,repeat('b',%d)) "  
        "on duplicate key update val=repeat(char(65+round(rand()*26)),%d)",
        db_num, tab_num,
         get_rand(&seed,max_key,2*max_key+min_key),
         blob_len,blob_len))
      {
        die("Error running insert query: %s - exiting ",
          mysql_error(mysql));
      }   
      t->insert_stats.add_response(resp_time);
    }

    for (j = 0; j < lr.updates; j++)
    {        
      int tab_num = get_rand(&seed,1,num_tables);
      int db_num = get_db(tab_num);

      if (timed_query(mysql,0,&resp_time,
        "update db_%d.t_%d set val=repeat(char(65+round(rand()*26)),%d)"
        " where k=%d",
        db_num, tab_num, blob_len,
        get_rand(&seed,min_key,max_key)))
      {
        die("Error running update query: %s - exiting ", 
          mysql_error(mysql));
      }   
      t->update_stats.add_response(resp_time);
    }

    for (j = 0; j < lr.deletes; j++)
    {        
      int tab_num = get_rand(&seed,1,num_tables);
      int db_num = get_db(tab_num);

      if (timed_query(mysql,0,&resp_time,
        "delete from db_%d.t_%d where k=%d",
        db_num, tab_num,
         get_rand(&seed,min_key,max_key)))
      {
        die("Error running delete query: %s - exiting ", 
          mysql_error(mysql));
      }   
      t->delete_stats.add_response(resp_time);
    }
    
    if (num_secondary_keys)
    {
      int sec_key_num = get_rand(&seed,1,num_secondary_keys);
      int table_num = get_rand(&seed,1,num_tables);
      int key_val = get_rand(&seed,min_key,max_key);

      for (j = 0; j < lr.selects; j++)
      {        
        mk_key_query(&seed,buf,sizeof(buf),table_num);
        
        if (timed_query(mysql,1,&resp_time,buf))
        {
          die("Error running secondary key select query:"
               " %s - exiting ", 
            mysql_error(mysql));
        }   
        t->select_sec_key_stats.add_response(resp_time);
        mk_join_query(&seed,buf,sizeof(buf),table_num);
        
        if (timed_query(mysql,1,&resp_time,buf))
        {
          die("Error running external key select query:"
               " %s - exiting ", 
            mysql_error(mysql));
        }   
        t->select_ext_key_stats.add_response(resp_time);
      }
      
      for (j = 0; j < lr.inserts; j++)
      {        
        mk_key_insert(&seed,buf,sizeof(buf),table_num);
        
        if (timed_query(mysql,0,&resp_time,buf))
        {
          die("Error running secondary key insert query:"
               " %s - exiting ", 
            mysql_error(mysql));
        }   
        t->insert_sec_key_stats.add_response(resp_time);
        mk_ext_insert(&seed,buf,sizeof(buf),table_num);
        
        if (timed_query(mysql,0,&resp_time,buf))
        {
          die("Error running secondary key insert query:"
               " %s - exiting ", 
            mysql_error(mysql));
        }   
        t->insert_ext_key_stats.add_response(resp_time);
      }

      for (j = 0; j < lr.updates; j++)
      {        
        mk_key_update(&seed,buf,sizeof(buf),table_num);
        
        if (timed_query(mysql,0,&resp_time,buf))
        {
          die("Error running secondary key update query:"
               " %s - exiting ", 
            mysql_error(mysql));
        }   
        
        t->update_sec_key_stats.add_response(resp_time);
        mk_ext_update(&seed,buf,sizeof(buf),table_num);
        
        if (timed_query(mysql,0,&resp_time,buf))
        {
          die("Error running secondary key update query:"
               " %s - exiting ", 
            mysql_error(mysql));
        }   
        t->update_ext_key_stats.add_response(resp_time);
      }

      char dbbuf[8192];
      snprintf(dbbuf, sizeof(dbbuf), "db_%d", get_db(table_num));
      if (mysql_select_db(mysql, dbbuf))
      { 
          die("Error connecting:"
               " %s - exiting ",
            mysql_error(mysql));
      }
 
      for (j = 0; j < lr.deletes; j++)
      {        
        mk_key_delete(&seed,buf,sizeof(buf),table_num);
        
        if (timed_query(mysql,0,&resp_time,buf))
        {
          die("Error running secondary key delete query:"
               " %s - exiting ", 
            mysql_error(mysql));
        }   
        
        t->delete_sec_key_stats.add_response(resp_time);
        mk_ext_delete(&seed,buf,sizeof(buf),table_num);
        
        if (timed_query(mysql,0,&resp_time,buf))
        {
          die("Error running secondary key delete query:"
               " %s - exiting ", 
            mysql_error(mysql));
        }   
        t->delete_ext_key_stats.add_response(resp_time);
      }
            
    }
  }  
err:  

  if (res)
  {
    mysql_free_result(res);
    res = 0;
  }
  
  mysql_close(mysql);
  t->signal_exit();
  return 0;
}


void parse_args(int argc,char** argv)
{
  int c;
  
  for (;;)
  {
    int opt_ind = 0;
    c = getopt_long(argc,argv,
       "h:u:D:P:p:r:t:T:S:k:K:b:s:o:j:d:U:w:l:x",
              opts,&opt_ind);  
    
    if (c == -1)
      return;
      
    switch (c)
    {
      case 'h':
        host = optarg;
        break;
      case 'u':
        user = optarg;
        break;
      case 'D':
        num_dbs = atoi(optarg);
        break;
      case 'S':
        sock = optarg;
        break;
      case 't':
        num_tables = atoi(optarg);
        break;
      case 'T':
        num_threads = atoi(optarg);
        break;
      case 'r':
        num_rounds = atoi(optarg);
        break;
      case 'p':
        pw = optarg;
        break;
      case 'P':
        port = atoi(optarg);
        break;
      case 'k':
        min_key = atoi(optarg);
        break;
      case 'K':
        max_key = atoi(optarg);
        break;
      case 'b':
        blob_len = atoi(optarg);
        break;
      case 's':
        num_secondary_keys = atoi(optarg);
        break;  
      case 'o':
        table_opts = optarg;
        break;  
      case 'j':
        num_joins = atoi(optarg);
        break;  
      case 'd':
        target_dir = optarg;
        break;  
      case 'U':
        sleep_between_queries = atoi(optarg);
        break;  
      case 'w':
        pre_warm = atoi(optarg);
        break;
      case 'l':
        load_ratios = optarg;
        break;    
      case 'x':
        no_xtrabackup = 1;
        break;  
      default:
        usage();     
    }  
  }
}

void print_stats()
{
  ulonglong total_reads, total_writes = 0;
  float read_throughput = 0.0, write_throughput = 0.0 ;
  
  total_reads = num_threads * num_rounds * 
   (lr.selects + 2 * (num_secondary_keys != 0) * lr.selects);
  total_writes = num_threads * num_rounds * 
   (lr.inserts + lr.deletes + lr.updates) *
     (1 + 2 * (num_secondary_keys != 0));
  
  if (test_time)
  {
    read_throughput = 1000000.0 * (float)total_reads/
        (float)test_time;
    write_throughput = 1000000.0 * (float)total_writes/
        (float)test_time;
    
  }
  
  msg_prefix = "";
  
  g_select_stats.print("Select stats:");
  g_insert_stats.print("Insert stats:");
  g_update_stats.print("Update stats:");
  g_delete_stats.print("Delete stats:");
    
  if (num_secondary_keys)
  {
    g_select_sec_key_stats.print("Select on secondary key stats:");
    g_select_ext_key_stats.print("Select on external key stats:");
    g_insert_sec_key_stats.print("Insert on secondary key stats:");
    g_insert_ext_key_stats.print("Insert on external key stats:");
    g_update_sec_key_stats.print("Update on secondary key stats:");
    g_update_ext_key_stats.print("Update on external key stats:");
    g_delete_sec_key_stats.print("Delete on secondary key stats:");
    g_delete_ext_key_stats.print("Delete on external key stats:");
    
    msg("Total query test time: %llu usec", test_time);
    msg("Read throughput: %.2f qps, write throughput: %.2f qps",
      read_throughput, write_throughput);
    
    msg("Line for report:"
     "|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|",
     read_throughput,
     write_throughput,
     g_select_sec_key_stats.get_avg_ms(),
     g_select_ext_key_stats.get_avg_ms(),
     g_insert_sec_key_stats.get_avg_ms(),
     g_insert_ext_key_stats.get_avg_ms(),
     g_update_sec_key_stats.get_avg_ms(),
     g_update_ext_key_stats.get_avg_ms(),
     g_delete_sec_key_stats.get_avg_ms(),
     g_delete_ext_key_stats.get_avg_ms()
     );
     
     
  }
}

int populate_ext_keys(MYSQL* mysql, int table_num)
{
 if (!num_secondary_keys || table_copy_exists("t_ext_k_1_", 
     table_num))
     return 0;
 
 int i;
 
 for (i = 1; i <= num_secondary_keys; i++)
 {
   int j;
   
   safe_queryf(glob_mysql,"begin");
   for (j = min_key; j <= max_key; j++)
   {
     safe_queryf(glob_mysql,"insert into db_%d.t_ext_k_%d_%d values("
     "%d,substring(repeat(md5(%d),%d),1,%d))",
     get_db(table_num),table_num,i,j,j,md5_repeat_count,sec_key_len);
   }
   safe_queryf(glob_mysql,"commit");
 }
     
 return 0;    
}

int populate_table_with_keys(MYSQL* mysql, int table_num)
{
  if (!num_secondary_keys || table_copy_exists("tk_",table_num))
     return 0;
   
 int i;
 char buf[2048];
 int pos;
 pos = snprintf(buf,sizeof(buf),"load data infile '%s/%s' into"
   " table db_%d.tk_%d fields terminated by ',' (k,val)", 
   datadir, data_fname,
   get_db(table_num),table_num);

 
 pos += snprintf(buf + pos, sizeof(buf) - pos, " set ");
 
 for (i = 0; i < num_secondary_keys; i++)
 {
   if (i)
   {
     if (pos >= sizeof(buf))
       die("Buffer overflow while populating tables");
    
     buf[pos++] = ',';   
   }  
   
   pos += snprintf(buf + pos, sizeof(buf) - pos, 
   " k%d = substring(repeat(md5(k),%d),1,%d) ", i + 1,
    md5_repeat_count, sec_key_len,
   i + 1);
 }
 
 safe_queryf(mysql,buf);      
 return 0;
}

int table_copy_exists(const char* prefix, int tab_num)
{
  if (!target_dir)
    return 0;
    
  char path[FILENAME_MAX];
  snprintf(path,sizeof(path),"%s/db_%d/%s%d.ibd", target_dir,
    get_db(tab_num),
    prefix, tab_num);
  if (access(path,R_OK))
  {
    msg("File %s does not exist", path);
    return 0;
  }  
  
  msg("File %s exists", path);
  return 1;  
}


int populate_table(MYSQL* mysql, int table_num)
{
  if (table_copy_exists("t_",table_num))
  {
    if (pre_warm)
    {
      timed_query(glob_mysql,1,0,"select count(*) from db_%d.t_%d",
		  get_db(table_num),table_num);
    }
    return 0;
  }  
  safe_queryf(mysql,"load data infile '%s/%s' into"
   " table db_%d.t_%d fields terminated by ','", datadir, data_fname,
   get_db(table_num),table_num);

  
  return 0;
}

void populate_file()
{
  char buf[FILENAME_MAX];
  FILE* fp;
  int i,j;
  
  if (!datadir)
    die("datadir not initalized while trying to populate file");
  
  snprintf(buf,sizeof(buf),"%s/%s", datadir, data_fname);
  
  if (!(fp = fopen(buf,"w")))
  {
    die("Could not open %s for writing", buf);
  }  
  
  for (i = min_key; i <= max_key; i++)
  {
    fprintf(fp,"%d,\"",i);
    
    for (j = 0; j < blob_len; j++)
    {
      fputc('a' + j % 26, fp);
    }
    
    fputc('"',fp);
    fputc('\n',fp);
  }
  
  fclose(fp);
}

#define PARSE_AND_ADVANCE(var,s) var=strtol(s,&s,10);\
  if (!*s) return; s++;

void Load_ratios::init_from_string(const char* arg)
{
  selects = inserts = updates = deletes = 1;
  char* s = (char*)arg;
  
  if (!s || !*s)
  {
    return;
  }
  
   PARSE_AND_ADVANCE(selects,s);  
   PARSE_AND_ADVANCE(inserts,s);  
   PARSE_AND_ADVANCE(updates,s);  
   PARSE_AND_ADVANCE(deletes,s);  
}

void simple_populate_tables()
{
  int i,j;
  for (i = 1; i <= num_tables; i++)
  {
    if (populate_table(glob_mysql,i))
    {
      glob_db_close();
      die("Could not populate table %d", i);
    }
      
    if (num_secondary_keys)
    {
      populate_table_with_keys(glob_mysql,i);
      populate_ext_keys(glob_mysql,i);
    }  
  }  
}

void populate_tables()
{
  int i,j;
  struct timeval tv_start, tv_end, tv_log_start, tv_log_end;
  Trx_info trx;
  char tables_arg[1024];

  gettimeofday(&tv_start,0);
  
  if (no_xtrabackup)
  {
     simple_populate_tables(); 
     goto end;
  }
  
  if (populate_table(glob_mysql,1))
  {
      glob_db_close();
      die("Could not populate table %d", i);
  }
  
  if (wait_for_log_flush)
  {
    gettimeofday(&tv_log_start,0);  
    for (;;)
    {
      if (get_trx_info(glob_mysql,&trx))
      {
        glob_db_close();
        die("Could not get trx info before sleep");
      }
      
      trx.print("Current InnoDB state"); 
      
      if (trx.lsn_current())
        break;
        
      sleep(xtrabackup_sleep);   
    }
    gettimeofday(&tv_log_end,0);
    
    printf("Log wait time: %llu usec\n", 
      get_time_diff(&tv_log_start,&tv_log_end));
  }
    
  if (num_secondary_keys)
  {
    populate_table_with_keys(glob_mysql,1);
    populate_ext_keys(glob_mysql,1);
  }
  
  
  if (!table_copy_exists("t_",1))
  {
    int db = get_db(1);

    snprintf(tables_arg, sizeof(tables_arg), 
      "db_%d.t_1\\$,db_%d.tk_1\\$,db_%d.t_ext_k_1_", db, db, db);
    
    if (backup_tables(datadir, innodb_log_dir, innodb_log_size, 
         tables_arg, target_dir))
    {
        glob_db_close();
        die("Could not backup with xtrabackup");
    }
  }
  
  for (i = 2; i <= num_tables; i++)
  {
    safe_queryf(glob_mysql,
      "alter table db_%d.t_%d discard tablespace", get_db(i), i);
      
    if (num_secondary_keys)
    {
      safe_queryf(glob_mysql,"alter table db_%d.tk_%d "
        " discard tablespace", get_db(i), i);
      for (j = 1; j <= num_secondary_keys; j++)
      {
        safe_queryf(glob_mysql,"alter table db_%d.t_ext_k_%d_%d "
           " discard tablespace", get_db(i), i, j);
      }
    }  
  }  
  
  copy_table_files("t_1","t_%d",2,num_tables);
  
  if (num_secondary_keys)
  {
    copy_table_files("tk_1","tk_%d",2,num_tables);
  
    for (i = 1; i <= num_secondary_keys; i++)
    {
      char src_buf[64], dst_buf[64];
      snprintf(src_buf,sizeof(src_buf),"t_ext_k_1_%d",i);
      snprintf(dst_buf,sizeof(dst_buf),"t_ext_k_%%d_%d",i);
      copy_table_files(src_buf,dst_buf,2,num_tables);
    }
  }  
  
  for (i = 2; i <= num_tables; i++)
  {
    import_tablespacef("db_%d.t_%d",get_db(i), i);
    
    if (num_secondary_keys)
    {
      import_tablespacef("db_%d.tk_%d",get_db(i), i);
       
      for (j = 1; j <= num_secondary_keys; j++)
      {
        import_tablespacef("db_%d.t_ext_k_%d_%d", get_db(i), i, j);
      }   
    }  
  }
  
end:  
  gettimeofday(&tv_end,0);
  
  printf("Time to populate tables: %llu usec\n", 
    get_time_diff(&tv_start,&tv_end));
}

void import_tablespacef(const char* tbl,...)
{
  char buf[FILENAME_MAX];
  va_list args;
  va_start(args,tbl);
  
  vsnprintf(buf,sizeof(buf),tbl,args);
  safe_queryf(glob_mysql, "alter table %s import tablespace", 
     buf);
     
  if (pre_warm)
  {
    timed_query(glob_mysql,1,0,"select count(*) from %s", buf);
  }
}


int main(int argc, char** argv)
{
  struct timeval tv_start,tv_end;
  do_exit_func = do_exit;

  printf("test started");
  fflush(stdout);

  parse_args(argc,argv); 
  lr.init_from_string(load_ratios);
  glob_db_connect();
  print_test_info();
  populate_file();
  setup_dbs_and_tables();
  setup_secondary_key_test();
  populate_tables();
  run_var_query("MySQL status variables of interest" 
   " after loading the data",
    "show status", mysql_status_vars, 0);
  
  glob_db_close();
  gettimeofday(&tv_start,0);
  start_threads(); 
  wait_for_threads();
  gettimeofday(&tv_end,0);
  test_time = get_time_diff(&tv_start,&tv_end);
  print_stats();
  cleanup();
  return 0;
}
