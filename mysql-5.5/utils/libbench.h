#ifndef LIBBENCH_H
#define LIBBENCH_H

#include "mysql.h"
#include <sys/time.h>
#include <time.h>

typedef unsigned long long ulonglong;

ulonglong get_time_diff(struct timeval* tv_start, 
  struct timeval *tv_end);
void die(const char *fmt, ...);
void warn(const char *fmt, ...);
void msg(const char *fmt, ...);
void file_msg(FILE* f, const char* fmt, va_list args);
int parse_size(long long* res, const char* str);

void safe_queryf(MYSQL* mysql,const char* fmt, ...);
 
extern void (*do_exit_func)(int);
extern const char* msg_prefix;

typedef int (*DB)(int);

struct Trx_info
{
  unsigned long counter_low;
  unsigned long counter_high;
  unsigned long purge_low;
  unsigned long purge_high;
  unsigned long undo_low;
  unsigned long undo_high;
  unsigned long history;
  unsigned long lsn_high;
  unsigned long lsn_low;
  unsigned long lsn_flushed_high;
  unsigned long lsn_flushed_low;
  unsigned long lsn_checkpoint_high;
  unsigned long lsn_checkpoint_low;
  
  void print(const char* msg_text)
  {
    msg("%s: counter %lu:%lu purge %lu:%lu undo %lu:%lu " 
     "history %lu lsn %lu:%lu lsn flushed %lu:%lu"
     " checkpoint %lu:%lu", 
       msg_text, counter_high, counter_low,
       purge_high, purge_low, undo_high, undo_low, history,
        lsn_high, lsn_low, lsn_flushed_high, lsn_flushed_low,
        lsn_checkpoint_high, lsn_checkpoint_low); 
  }
  
  int lsn_current()
  {
    if (lsn_high > lsn_checkpoint_high)
      return 0;
    if (lsn_high < lsn_checkpoint_high)
      return 1;
      
    return lsn_low <= lsn_checkpoint_low;  
  }
};

int copy_files(const char* src, const char* dst, int start_ind,
  int end_ind, const char* ext, DB);
int backup_tables(const char* datadir, const char* innodb_log_dir, 
  unsigned long long log_file_size,
  const char* tables_arg,
  const char* target_dir);

int get_trx_info(MYSQL* mysql, Trx_info *trx);
  
struct Response_stats
{
  ulonglong max_time;
  ulonglong min_time;
  ulonglong total_time;
  ulonglong count;
  struct timeval tv_start, tv_end;
  
  Response_stats():max_time(0),min_time(0),total_time(0),
   count(0) { }
   
  void start_timer()
  {
    gettimeofday(&tv_start,0);
  }
  
  void stop_and_record()
  {
    gettimeofday(&tv_end,0);
    add_response(get_time_diff(&tv_start,&tv_end));
  }
   
  void add_response(ulonglong t)
  {
    if (!max_time || t > max_time)
      max_time = t;
      
    if (!min_time || t < min_time)
      min_time = t;
      
    count++;  
    total_time += t;
  }
  
  void add(Response_stats* other)
  {
    if (!max_time || other->max_time > max_time)
      max_time = other->max_time;
    if (!min_time || other->min_time < min_time)
      min_time = other->min_time;
    
    count += other->count;
    total_time += other->total_time;
  }
  
  float get_avg_ms()
  {
    if (!count) return 0.0;
    return (float)total_time/(1000.0*(float)count);
  }
  
  void print(const char* msg_txt)
  {
    msg(msg_txt);
    msg("Min time: %llu usec", min_time);
    msg("Max time: %llu usec", max_time);
    msg("Avg time: %llu usec", count ? total_time/count : 0);
    msg("Total time: %llu usec",  total_time);
    msg("Executions: %llu ",  count);
    msg("");
  }
};

#endif
