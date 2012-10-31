#ifndef DBUS_TCP_CLIENT_H
#define DBUS_TCP_CLIENT_H

#include "dbus_client.h"
#include "dbus_config.h"

class Dbus_tcp_client: public Dbus_client
{
  public:
    Dbus_tcp_client(const char* host, uint port):
     Dbus_client(host,port) {}
    virtual ~Dbus_tcp_client(){}
    
    virtual int shake_hands(Dbus_config* conf);
    virtual int store_event(Dbus_event* e) ;
    virtual int read_response();
    virtual int store_sources(Dbus_config* conf, String& s);
    
};

#endif
