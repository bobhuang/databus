#include <stdio.h>
#include <stdarg.h>
#include <sys/time.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include "libbench.h"

const char* msg_prefix = 0;
void (*do_exit_func)(int) = 0;

extern int get_db(int tab_num);

static inline const char* find_next_digit(const char* s)
{
  for (;*s;s++)
  {
    if (isdigit(*s))
      return s;
  }
  
  return 0;
}

static inline const char* parse_number_pair(const char* s,
 const char* tag,
 unsigned long *high, unsigned long *low)
{
  if (!(s = strstr(s,tag)))
    return 0;
    
  if (!(s = find_next_digit(s)))  
     return 0;
  
  *high = strtoul(s,(char**)&s,10);   

  if (*s == '\n' || !isdigit(s[1]))
  {
    *low = *high;
    *high = 0;
    return s;
  }
  else
    s++;
     
  *low = strtoul(s,(char**)&s,10);   
  return s;
}


void msg(const char* fmt, ...)
{
  va_list args;
  va_start(args,fmt);
  file_msg(stdout,fmt,args);
  va_end(args);
}

void warn(const char* fmt, ...)
{
  va_list args;
  msg_prefix = 0;
  va_start(args,fmt);
  file_msg(stderr,fmt,args);
  va_end(args);
}

ulonglong get_time_diff(struct timeval* tv_start, 
  struct timeval *tv_end)
{
  return ((ulonglong)tv_end->tv_sec -
   (ulonglong)tv_start->tv_sec) *
    1000000 +    (ulonglong)tv_end->tv_usec -
     (ulonglong)tv_start->tv_usec;
}  


void file_msg(FILE* f, const char *fmt, va_list args)
{
  if (!msg_prefix)
    fprintf(f, "multi-table-bench: ");
  else
    fputs(msg_prefix,f);  
    
  if (fmt)
  {
    vfprintf(f, fmt, args);
  }
  
  fprintf(f, "\n");
  fflush(f);
}

int parse_size(long long* res, const char* str)
{
  char* p;
  *res = strtoll(str,&p,10);
  
  switch (tolower(*p))
  {
    case 0:
      return !(*res > 0);
    case 'k':
      *res *= 1024;
      break;
    case 'm':
      *res *= 1024 * 1024;
      break;
    case 'g':
      *res *= 1024 * 1024 * 1024;
      break;
    default:
      return 1;  
  }
  
  return 0;
}

int copy_files(const char* src, const char* dst, 
 int start_ind,  int end_ind, const char* ext, DB fp)
{
  int src_fd = -1, *dst_fds = 0;
  char buf[4096];
  int res = -1;
  int read_bytes, written_bytes;
  int i;
  int num_copies = end_ind - start_ind + 1;
  
  if (num_copies <= 0)
    return 0;
  
  dst_fds = (int*)malloc(num_copies *sizeof(int));
  
  if (!dst_fds)
  {
    warn("Out of memory");
    return -1;
  }
  
  for (i = 0; i < num_copies; i++)
  {
    dst_fds[i] = -1;
  }
  
  if ((src_fd = open(src,O_RDONLY)) < 0)
  {
    warn("Could not open source file %s", src);
    return -1;
  }  
  
  for (i = 0; i < num_copies; i++)
  {
    char fname[FILENAME_MAX];
    char tmp[FILENAME_MAX];
    snprintf(tmp,sizeof(tmp),dst, (*fp)(i+start_ind),i+start_ind);
    snprintf(fname, sizeof(fname), "%s%s",tmp, 
      ext);
    
    if ((dst_fds[i] = open(fname,O_WRONLY|O_CREAT,0660)) < 0)
    {
      warn("Could not open destination file %s: errno %d",
        fname, errno);
      goto err;   
    }
  }
  for (;;)
  {
    read_bytes = read(src_fd, buf, sizeof(buf));
    
    if (read_bytes < 0)
    {
      warn("Error %d reading file", errno);
      goto err;
    }
    
    if (read_bytes == 0)
      break;
    
    for (i = 0; i < num_copies; i++)
    {    
      written_bytes = write(dst_fds[i], buf, read_bytes);
    
      if (written_bytes != read_bytes)
      {
        warn("Wrote %d bytes instead of %d", written_bytes,
          read_bytes);
        goto err;  
      }  
    }    
  }
  
  res = 0; 
err:  
  
  if (src_fd >= 0)
    close(src_fd);
    
  for (i = 0; i < num_copies; i++)
  {
    if (dst_fds[i] >= 0)
      close(dst_fds[i]);
  }
  
  if (dst_fds)
    free(dst_fds);
      
  return res;  
}

int backup_tables(const char* datadir, const char* innodb_log_dir,
   unsigned long long log_file_size,
   const char* tables_arg,
  const char* target_dir)
{
  char cmd[1024];
  int err_code;
  snprintf(cmd, sizeof(cmd), "xtrabackup --no-defaults "
  " --backup --target-dir=%s --datadir=%s --tables=%s"
  " --innodb-data-home-dir=%s --innodb-log-group-home-dir=%s"
  " --innodb-file-per-table --innodb-log-file-size=%llu "
  ,
  target_dir, datadir, tables_arg, datadir, innodb_log_dir,
   log_file_size);
  
  if ((err_code=system(cmd)))
  {
    warn("Shell command '%s' failed with status %d", cmd, err_code);
    return 1;
  }
  
  snprintf(cmd, sizeof(cmd), "xtrabackup --no-defaults "
    " --prepare --export --innodb-file-per-table --target-dir=%s"
    " --tables=%s --datadir=%s  ",
     target_dir, tables_arg,
    datadir);
  
  //die("Debug exit, now run %s manually", cmd);
  
  if ((err_code=system(cmd)))
  {
    warn("Shell command '%s' failed with status %d", cmd, err_code);
    return 1;
  }

  return 0;    
}  

int get_trx_info(MYSQL* mysql, Trx_info* trx)
{
  MYSQL_RES* res;
  MYSQL_ROW row;
  int ret_code = -1;
  const char* status = 0;
  const char* p = 0;
  
  safe_queryf(mysql,"show engine innodb status");
  
  if (!(res = mysql_store_result(mysql)))
  {
    warn("Error storing result while getting trx info");
    return -1;
  }
  
  if (mysql_num_fields(res) < 3)
  {
    warn("Not enough fields while fetching trx info");
    goto err;
  }
  
  row = mysql_fetch_row(res);
  
  if (!row)
  {
    warn("No rows in innodb status");
    goto err;
  }
  
  status = row[2];
  msg("Status: %s", status);
  memset(trx,0,sizeof(trx));
  
  
  p = parse_number_pair(status,"Trx id counter",
    &trx->counter_high,&trx->counter_low);
  
  if (!p)
  {
    warn("Could not find Trx id counter value "
       " in innodb status");
    goto err;
  }
  
  p = parse_number_pair(p,"Purge done",
    &trx->purge_high,&trx->purge_low);
  
  if (!p)
  {
    warn("Could not find purge value "
       " in innodb status");
    goto err;
  }
  p = parse_number_pair(p,"undo",&trx->undo_high,&trx->undo_low);
  
  if (!p)
  {
    warn("Could not find undo value "
       " in innodb status");
    goto err;
  }
  
  p = strstr(p, "History list length");
  
  if (!p)
  {
    warn("Could not find History list length in innodb status");
    goto err;
  }
  
  p = find_next_digit(p);
  
  if (!p)
  {
    warn("Could not find history value in innodb status");
    goto err;
  }
  
  trx->history = strtoul(p,NULL,10);
  
  p = parse_number_pair(p,"Log sequence number", &trx->lsn_high,
    &trx->lsn_low);
    
  if (!p)
  {
    warn("Could not find Log sequence number value" 
    " in innodb status");
    goto err;
  }
  
  p = parse_number_pair(p,"Log flushed up to", 
    &trx->lsn_flushed_high,
    &trx->lsn_flushed_low);
    
  if (!p)
  {
    warn("Could not find Log flushed up to value" 
    " in innodb status");
    goto err;
  }
  
  p = parse_number_pair(p,"Last checkpoint at", 
    &trx->lsn_checkpoint_high,
    &trx->lsn_checkpoint_low);
    
  if (!p)
  {
    warn("Could not find Last checkpoint at value" 
    " in innodb status");
    goto err;
  }
    
  ret_code = 0;
err:  
  mysql_free_result(res);
  return ret_code;
}


void safe_queryf(MYSQL* mysql,const char* fmt, ...)
{
  char buf[8192];
  int query_len;
  va_list args;
  va_start(args, fmt);
  query_len = vsnprintf(buf, sizeof(buf), fmt, args);
  va_end(args);
  
  if (mysql_real_query(mysql,buf,query_len))
  {
    die("Error running query '%s': %s", buf,
     mysql_error(mysql));
  }
}

void die(const char *fmt, ...)
{
  va_list args;

  if (msg_prefix)
    fprintf(stderr, "%s: ", msg_prefix);
    
  if (fmt)
  {
    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    va_end(args);
  }
  
  fprintf(stderr, "\n");
  fflush(stderr);
  
  if (do_exit_func)
   (*do_exit_func)(1);
  else
    exit(1); 
}
