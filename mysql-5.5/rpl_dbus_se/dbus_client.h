#ifndef DBUS_CLIENT_H
#define DBUS_CLIENT_H

#include "tcp_socket.h"
#include "dbus_config.h"

#define INIT_EVENT_SIZE 2048

#define END_OF_WINDOW_SRC_ID -2

struct Dbus_event
{
  int opcode;
  ulonglong lsn;
  ulonglong ns_ts;
  int phys_part_id;
  int l_part_id;
  int src_id;
  const char* schema_id;
  const char* key;
  uint key_len;
  const char* val;
  uint val_len;
  ulonglong binlog_id;
  
  void print( const char* msg);
};



class Dbus_client: public Tcp_socket
{
  protected:

    ulonglong last_lsn;
    bool hands_shaken;
    uint max_trans_size;
    uint max_num_events;
    ulonglong resp_lsn;
    ulonglong check_resp_lsn;
    uint server_id;
    String buffer;
    char buf[INIT_EVENT_SIZE]; 
    bool lsn_change_requested;
    uint num_sources;
  
  public:
    Dbus_client(const char* host, uint port);
    virtual ~Dbus_client(); 
    
    int ensure_hands_shaken(Dbus_config *conf);
    virtual int shake_hands(Dbus_config *conf) = 0;
    virtual int store_event(Dbus_event* e) = 0;
    virtual int read_response() = 0;
    int reinit() { hands_shaken = 0; return Tcp_socket::reinit();}
    int send_event(Dbus_config *conf);
    int store_and_send(Dbus_config* conf, Dbus_event* e);
    void set_last_lsn(ulonglong arg) { last_lsn = arg; }
    ulonglong get_resp_lsn() { return resp_lsn; }
    bool lsn_change_is_requested() { return lsn_change_requested;}
    bool event_is_good(Dbus_event* e);
    void set_num_sources(uint num_sources_arg) 
    { num_sources = num_sources_arg; }
    void set_server_id(uint server_id_arg) { server_id = server_id_arg;}
    
    void request_new_handshake() { hands_shaken = 0; }
};

#endif
