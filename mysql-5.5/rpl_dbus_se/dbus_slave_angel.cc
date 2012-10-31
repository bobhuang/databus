#include <my_pthread.h>

#include "dbus_slave_angel.h"
#include "log.h"
#include "slave.h"
#include "sql_repl.h"
#include "mysqld.h"
#include "rpl_mi.h"
#include "sql_priv.h"
#include "sql_lex.h"

extern PSI_mutex_key  key_mutex_dbus_slave_angel;
extern PSI_thread_key key_thread_dbus_slave_angel;
extern PSI_cond_key key_cond_dbus_slave_angel;

static int init_angel_thd(THD* thd);

static void* handle_angel(void* arg);

static int init_angel_thd(THD* thd)
{
  thd->system_thread = SYSTEM_THREAD_EVENT_WORKER;
  thd->security_ctx->skip_grants();
  my_net_init(&thd->net, 0);
  mysql_mutex_lock(&LOCK_thread_count);
  thd->thread_id= thd->variables.pseudo_thread_id= thread_id++;
  mysql_mutex_unlock(&LOCK_thread_count);
  
  if (init_thr_lock() || thd->store_globals())
  {
    thd->cleanup();
    return 1;
  }
  
  return 0;
}

static void* handle_angel(void* arg)
{
  Dbus_slave_angel* a = (Dbus_slave_angel*)arg;
  a->run();
  return 0;
}


int Dbus_slave_angel::init()
{
  target_lsn = 0;
  done = 0;
  thd = 0;
  mysql_mutex_init(key_mutex_dbus_slave_angel,
    &the_lock, MY_MUTEX_INIT_FAST);
  mysql_cond_init(key_cond_dbus_slave_angel,&cond,NULL); 
  inited = 1;
  return 0; 
}

int Dbus_slave_angel::clean()
{
  if (!inited)
    return 0;
    
  delete thd;
  thd = 0;
  mysql_mutex_destroy(&the_lock);
  mysql_cond_destroy(&cond);
  inited = 0;
  return 0;
}

void Dbus_slave_angel::signal_end_wait()
{
  lock();
  waiting = 0;
  mysql_cond_broadcast(&cond);
  unlock();
}


void Dbus_slave_angel::wait()
{
  bool need_unlock = 1;
  lock();
  waiting = 1;
  
  while (waiting)
  {
    need_unlock = 0;
    sql_print_information("D-BUS slave angel - waiting");
    const char* old_msg = thd->enter_cond(&cond,&the_lock, 
      "D-BUS slave angel waiting to be woken up");
    mysql_cond_wait(&cond,&the_lock);
    thd->exit_cond(old_msg);
    
    if (thd->killed || (thd->mysys_var && thd->mysys_var->abort ) 
     || abort_loop)
      break;
      
    lock();  
    need_unlock = 1;
  }
  
  if (need_unlock)
    unlock();
}

   
void Dbus_slave_angel::lock()
{
  mysql_mutex_lock(&the_lock);
}

void Dbus_slave_angel::unlock()
{
  mysql_mutex_unlock(&the_lock);
}
   
int Dbus_slave_angel::start()
{
  if (mysql_thread_create(key_thread_dbus_slave_angel,&th,
    &connection_attrib,handle_angel,(void*)this))
  {
    done = 1;
    sql_print_error("Could not create D-BUS slave angel thread");
    return 1;
  }  
  
  return 0;
}

int Dbus_slave_angel::request_stop()
{
  if (thd)
    thd->killed = THD::KILL_CONNECTION;
  return 0;
}

int Dbus_slave_angel::run()
{
#ifndef EMBEDDED_LIBRARY
  char* stack_var;
  my_thread_init();
  done = 0;
  
  if (thd)
    delete thd;
    
  thd = new THD;
  
  if (!thd)
  {
    my_thread_end();
    return 1;
  }
  
  if (init_angel_thd(thd))
  {
    sql_print_error("D-BUS slave angel: initialization error");
    goto err;
  }
        
  thd->thread_stack = (char*)&stack_var;
  mysql_mutex_lock(&LOCK_thread_count);
  threads.append(thd);
  mysql_mutex_unlock(&LOCK_thread_count);

  sql_print_information("D-BUS slave angel: starting");
  
  while (!thd->killed && !abort_loop)
  {
    wait();
    
    if (thd->killed || abort_loop)
      break;
      
    sql_print_information("D-BUS slave angel woken up,"
      " target_lsn=%llu", target_lsn);
      
    mysql_mutex_lock(&LOCK_active_mi);  
    if (ensure_slave_stopped() == 0)
       start_slave();
    mysql_mutex_unlock(&LOCK_active_mi); 
    target_lsn = 0;
  }

err:  
  sql_print_information("D-BUS slave angel: terminating");
  delete thd;
  thd = 0;
  done = 1;
  lock();
  mysql_cond_broadcast(&cond);
  unlock();
  my_thread_end();
#endif  
  return 0;
}

int Dbus_slave_angel::ensure_slave_stopped()
{
#ifndef EMBEDDED_LIBRARY
 Master_info* mi = active_mi;
  
 if (!mi)
 {
   sql_print_information("D-BUS angel: Slave not configured");
   return 1;
 }

  return stop_slave(thd,mi,0);
#else
  return 0;  
#endif  
}

int Dbus_slave_angel::init_master_info()
{
#ifndef EMBEDDED_LIBRARY
  LEX* lex = thd->lex;
  Master_info* mi = active_mi;
  lex->sql_command = SQLCOM_CHANGE_MASTER;
  bzero((char*) &lex->mi, sizeof(lex->mi));
  my_init_dynamic_array(&lex->mi.repl_ignore_server_ids,
                                  sizeof(::server_id), 16, 16);
                                  
  lex->mi.pos = (target_lsn & 0xffffffff);
  
  if (!mi->rli.group_master_log_name)
    return 1;
    
  strncpy(buf,mi->rli.group_master_log_name,sizeof(buf));
  char* buf_end = buf + strlen(buf);
  
  char* dot = strrchr(buf,'.');                                 
  
  if (!dot)
    return 1;
  
  char* p;
  char pos_buf[22];
    
  llstr(target_lsn >> 32,pos_buf);  
  uint pos_len = strlen(pos_buf);
  char* pos_p = pos_buf + pos_len - 1;
  
  for (p = buf_end - 1; p > dot; p--)
  {
    if (pos_p >= pos_buf)
    {
      *p = *pos_p--;
    }
    else
    {
      *p = '0';
    }
  }
  
  lex->mi.log_file_name = buf;
#endif  
  return 0;
}

int Dbus_slave_angel::start_slave()
{
#ifndef EMBEDDED_LIBRARY
  if (!target_lsn)
    return 0;
    
  if (init_master_info())
  {
    sql_print_error("D-BUS angel: Error in init_master_info()");
    return 1;
  }  
  
  if (change_master(thd,active_mi))
  {
    sql_print_error("D-BUS angel: Error in change_master()");
    return 1;
  }  
  
  // cleanup
  thd->lex->mi.pos = 0;
  thd->lex->mi.log_file_name = 0;
  
  if (::start_slave(thd,active_mi,0))
  {
    sql_print_error("D-BUS angel: Error in start_slave()");
    return 1;
  }  
#endif  
  return 0;  
}

int Dbus_slave_angel::terminate()
{
  if (!inited || done)
    return 0;
  request_stop();
  signal_end_wait();
  
  lock();
  while (!done)
    mysql_cond_wait(&cond,&the_lock);
  unlock();
  return 0;
}
