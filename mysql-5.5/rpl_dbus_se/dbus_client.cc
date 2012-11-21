
#include "dbus_slave_angel.h"
#include "dbus_state.h"
#include "dbus_config.h"
#include "dbus_client.h"
#include "log.h"

extern ulong rpl_dbus_debug;
extern Dbus_state* dbus_state;
extern Dbus_slave_angel* dbus_slave_angel;

Dbus_client::Dbus_client(const char* host, 
   uint port):Tcp_socket(host,port),last_lsn(0),hands_shaken(0),
   max_trans_size(0),max_num_events(0),resp_lsn(0),
   check_resp_lsn(0),server_id(0),lsn_change_requested(0),num_sources(0)
{
   buffer.set((char*)buf, sizeof(buf), &my_charset_bin);
   buffer.length(0);
}

Dbus_client::~Dbus_client()
{
}
    
int Dbus_client::ensure_hands_shaken(Dbus_config *conf)
{
  lsn_change_requested = 0;
  
  for (;;)
  {
    if (ensure_connected())
    {
      sql_print_error("Cannot shake hands, no connection");
      return 1;
    }
    
    if (hands_shaken)
      return 0;
    
    if (rpl_dbus_debug)
      sql_print_information("Shaking hands with relay");
      
    if (shake_hands(conf))
    {
      sql_print_error("D-BUS handshake failed: %s(%d)",
        error_msg.c_ptr(), last_errno);
      reinit();
      safe_sleep();
      continue;
    }  
    else
      break;
  }
  
  hands_shaken = 1;
  return 0;
}

int Dbus_client::send_event(Dbus_config *conf)
{
  for (;;)
  {
    if (ensure_hands_shaken(conf))
    {
      sql_print_error("Cannot send D-Bus event - no handshake");
      return 1;
    }
    
    if (lsn_change_is_requested())
    {
      return 0;
    }
    
    if (write(&buffer,buffer.length()))
    {
      sql_print_error("D-Bus write failed, retrying");
      reinit();
      if (safe_sleep())
      {
        sql_print_information("Thread exit requested, aborting retries");
        return 1;
      }
      
      continue;
    }
    
    if (read_response())
    {
      sql_print_error("Bad response to event message: %s(%d)," "retrying", error_msg.c_ptr(), last_errno);
      reinit();
      if (safe_sleep())
      {
        sql_print_information("Thread exit requested, aborting retries");
        return 1;
      }
      
      continue;
    }
    
    break;
  }
  
  return 0;
}

void Dbus_event::print( const char* msg)
{
  fputs(msg,stderr);
  fprintf(stderr,"[op=%d,lsn=%llu,ns_ts=%llu,phys_part_id=%d,l_part_id=%d"
    ",src_id=%d,key_len=%d,val_len=%d,binlog_id=%llu]\n",
     opcode,lsn,ns_ts,phys_part_id,
     l_part_id,src_id,key_len,val_len,binlog_id);
  fflush(stderr);
}

bool Dbus_client::event_is_good(Dbus_event* e)
{
  if (e->phys_part_id < 0) /* relay cannot handle those regardless */
    return 0; 
    
  /* if the above is OK, this is end_of_window, accept it  */  
  if (e->src_id == END_OF_WINDOW_SRC_ID) 
    return 1;
  
  return 1; /* for now, that's all */
}

int Dbus_client::store_and_send(Dbus_config* conf, Dbus_event* e)
{
  if (!event_is_good(e))
  {
    e->print("RPL_DBUS: Bad event follows:");
    sql_print_error("RPL_DBUS: bad event will not be sent to the relay: %s",
       error_msg.c_ptr());
    return 0;   
  }
  
  if (store_event(e))
  {
    sql_print_error("Out of memory storing event");
    return 1;
  }
  
  if (send_event(conf))
  {
     sql_print_error("D-BUS send_event() failed: %s(%d)",
       error_msg.c_ptr(), last_errno);
     return 1;
  }
  
  if (lsn_change_is_requested())
  {
    ulonglong target_lsn = get_resp_lsn();
    
    sql_print_information(
     "D-BUS server requested LSN change to %llu",
      target_lsn);
    dbus_slave_angel->set_target_lsn(target_lsn);
    dbus_state->set_lsn(target_lsn);
    dbus_state->flush(); 
    dbus_slave_angel->signal_end_wait();
    return 1;
  }
  
  dbus_state->set_lsn(e->lsn);
  dbus_state->flush();

  return 0;
}
