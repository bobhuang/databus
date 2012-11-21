#ifndef DBUS_STATE_H
#define DBUS_STATE_H

#define DBUS_STATE_FNAME "rpl_dbus.info"
#define DBUS_STATE_SYNC_INTERVAL 5

#include "my_global.h"



class Dbus_state
{
  protected:
    bool inited;
    char* fname;
    int fd;
    ulonglong lsn;
    time_t last_sync_ts;
    uint sync_interval;
    
  public:
    Dbus_state():inited(0),last_sync_ts(0),
      sync_interval(DBUS_STATE_SYNC_INTERVAL) {}
    ~Dbus_state() { clean();}
    
    int init();
    int clean();
    int flush();
    
    void set_lsn(ulonglong arg) { lsn = arg; }
    ulonglong get_lsn() { return lsn; }
    
    bool is_inited() { return inited; }
};


#endif