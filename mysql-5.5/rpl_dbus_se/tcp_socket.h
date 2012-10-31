#ifndef TCP_SOCKET_H
#define TCP_SOCKET_H

#include "violite.h"
#include "sql_string.h"

class String;

class Tcp_socket
{
  public:
    Vio* vio;
    const char* host;
    uint port;
    uint connect_timeout;
    uint read_timeout;
    uint write_timeout;
    uint connect_retry_wait;
    bool connected;
    String error_msg;
    uint last_errno;
    bool inited;
    bool inited_from_config;
    char msgbuf[512];
    
    Tcp_socket(const char* host, uint port, int sock=-1);
    ~Tcp_socket();
    
   int init(int sock);

    // return code: 0 - success, non-zero - error
    int connect();
    // return code: < 0 - error, > 0 - bytes read
    int read(String* buf, uint max_len) 
    {
      return read((char*)buf->ptr(), max_len);
    }
    int read(char* buf, uint max_len);

    // return code: < 0 - error, 0 - success    
    int read_fully(char* buf, uint len);

    // return code: < 0 - error, 0 - success
    int write(String* buf, uint len)
    {
      return write(buf->ptr(),len);
    }   
    int write(const char* buf, uint len);   
    int close();
    
    int ensure_connected();
    int safe_write(String* buf, uint len);
    
    virtual int reinit()
    {
      close();
      return init(-1);
    }
    
    void store_error(const char* msg, uint err_code)
    {
      error_msg.length(0);
      error_msg.append(msg);
      last_errno = err_code;
    }
    
    void set_port(uint arg)
    {
      port = arg;
    }
    
    void set_connect_timeout(uint arg)
    {
      connect_timeout = arg;
    }
    
    void set_read_timeout(uint arg)
    {
      read_timeout = arg;
    }
    
    void set_write_timeout(uint arg)
    {
      write_timeout = arg;
    }
    void set_connect_retry_wait(uint arg)
    {
      connect_retry_wait = arg;
    }
    
    void set_host(const char* arg)
    {
      host = arg;
    }
    
    int safe_sleep(uint sec=0);
};


#endif