#ifndef DBUS_SLAVE_ANGEL_H
#define DBUS_SLAVE_ANGEL_H

#include "my_global.h"
#define MYSQL_SERVER 1
#include "sql_class.h"
#include "my_sys.h"
#include <stdio.h>

class Dbus_slave_angel
{
 protected:
  THD* thd;
  ulonglong target_lsn;
  mysql_mutex_t the_lock;
  mysql_cond_t cond;
  pthread_t th;
  bool inited;
  volatile bool waiting;
  char buf[FILENAME_MAX];
  volatile bool done;
  
 public:
   Dbus_slave_angel() { init(); }
   ~Dbus_slave_angel() { clean(); } 
   
   void lock();
   void unlock();
   void wait();
   void signal_end_wait();
   
   void set_target_lsn(ulonglong arg)
   {
     lock();
     target_lsn = arg;
     unlock();
   }
   
   THD* get_thd() { return thd; }
   
   bool is_inited() { return inited; }
   int init();
   int clean();
   
   int start();
   int request_stop();
   int run();
   
   int ensure_slave_stopped();
   int start_slave();
   int init_master_info();
   int terminate();
};

#endif