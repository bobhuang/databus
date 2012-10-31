#define MYSQL_SERVER
#include <my_global.h>
#include "sql_class.h"
#include <violite.h>
#include <my_pthread.h>
#include <thr_alarm.h>
#include <sql_string.h>
#include <mysqld.h>
#include <mysql_com.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

#include "slave.h"
#include "rpl_mi.h"
#include "rpl_rli.h"
#include "sql_repl.h"

#include "tcp_socket.h"

#define SOCKET_ERROR -1
#define TCP_READ_TIMEOUT 5
#define TCP_WRITE_TIMEOUT 5
#define TCP_CONNECT_TIMEOUT 5
#define TCP_CONNECT_RETRY_WAIT  5
#define TCP_RETRIES  5

static int net_data_is_ready(my_socket sd, int timeout,
  int event_mask);

static int tcp_safe_sleep(THD* thd, int sec);
static int slave_killed(THD* thd);

static int slave_killed(THD* thd)
{
#ifdef HAVE_REPLICATION
  return abort_loop || thd->killed || (active_mi && 
    active_mi->rli.abort_slave);
#else
  return 0;
#endif      
}


Tcp_socket::Tcp_socket(const char* host, uint port, int sock):
 vio(0),host(host),port(port),
 connect_timeout(TCP_CONNECT_TIMEOUT),
 read_timeout(TCP_READ_TIMEOUT),
 write_timeout(TCP_WRITE_TIMEOUT),
 connect_retry_wait(TCP_CONNECT_RETRY_WAIT),
 connected(0),
 last_errno(0),
 inited(0),inited_from_config(0)
{
  if (!init(sock)) // success
   inited = 1;
}

Tcp_socket::~Tcp_socket()
{
  vio_delete(vio);
  vio = 0;
  inited = 0;
}

int Tcp_socket::init(int sock)
{
  if (inited) 
    return 0; // nothing to do
  
  vio = 0;
  last_errno = 0;
  error_msg.set(msgbuf, sizeof(msgbuf), &my_charset_latin1);
  error_msg.length(0);  

  if (sock < 0) 
   sock= socket(AF_INET, SOCK_STREAM, 0);
  
  if (sock == SOCKET_ERROR)
  {
    last_errno = errno;
    error_msg.append("Error creating socket");
    return 1;
  }
  
  if (!(vio = vio_new(sock,VIO_TYPE_TCPIP,0)))
  {
    last_errno = errno;
    error_msg.append("Error creating VIO");
    ::close(sock);
    return 1;
  }
  
  return 0;
}

int Tcp_socket::connect()
{
  struct addrinfo *res_lst, hints, *t_res;
  int gai_errno;
  memset(&hints, 0, sizeof(hints));
  hints.ai_socktype= SOCK_STREAM;
  hints.ai_protocol= IPPROTO_TCP;
  hints.ai_family= AF_INET;
  char port_buf[NI_MAXSERV];
  
  my_snprintf(port_buf, NI_MAXSERV, "%d", port);
  gai_errno= getaddrinfo(host, port_buf, &hints, &res_lst);
  
  error_msg.length(0);
  if (gai_errno)
  {
    error_msg.append("Host name resolution error");
    last_errno = gai_errno;
    return 1;
  }
  
  t_res = res_lst;
  int flags = fcntl(vio->sd, F_GETFL, 0);
  fcntl(vio->sd, F_SETFL, flags | O_NONBLOCK);
     
  for (;;)
  {
    int s_err;
    
    
    int res = ::connect(vio->sd, t_res->ai_addr, t_res->ai_addrlen);
    
    if (!res)
    {
      break;     
    }
    s_err = errno;
                       
    if (s_err != EINPROGRESS)
    {
      error_msg.append("Connect error");
      last_errno = errno;
      goto err;
    }              
    else
    {
      res = net_data_is_ready(vio->sd,connect_timeout,
        POLLOUT);
      
      if (res)
        break;
      else
      {
          
        error_msg.append("Connect error during select()");
        last_errno = errno;
        goto err;  
      }  
    }
  }
  fcntl(vio->sd, F_SETFL, flags);
  connected = 1;
err:  
  freeaddrinfo(res_lst);
  return !connected;
}

int Tcp_socket::read_fully(char* buf, uint len)
{
  uint bytes_left = len;

  for (;;)
  {
    int bytes_read = read(buf, bytes_left);
    
    if (bytes_read <= 0 )
      return -1; // error
    
    if ((uint)bytes_read >= bytes_left)
      return 0;
    
    bytes_left -= bytes_read;
    buf += bytes_read;
  }
}

int Tcp_socket::read(char* buf, uint max_len)
{
  int bytes_read = 0;
  my_bool old_mode;
  
  if (vio_blocking(vio,FALSE,&old_mode)<0)
  {
    store_error("Could not set the socket to non-blocking mode",
      errno);
    return 0;
  }
  
  if (!net_data_is_ready(vio->sd,read_timeout,POLLIN))
  {
    sql_print_error("Socket read timed out (%d)", errno);
    goto err;
  }
  bytes_read = vio_read(vio,(uchar*)buf,max_len); 
  if (bytes_read <= 0)
    goto err;
  
  return bytes_read;
err:
  return -1;
}

int Tcp_socket::write(const char* buf, uint len)
{
  thr_alarm_t alarmed;
  ALARM alarm_buff;
  int retries_left = TCP_RETRIES;
  uchar* pos = (uchar*)buf, *end;
  int res = -1;
  my_bool old_mode;
  
  end = pos + len;
  
  if (vio_blocking(vio,TRUE,&old_mode)<0)
  {
    sql_print_error("Could not set the socket to blocking mode");
    return 0;
  }
  
  thr_alarm_init(&alarmed);
  thr_alarm(&alarmed, write_timeout, &alarm_buff);
  
  while (pos < end)
  {
    int bytes_written = vio_write(vio,pos,len);
    
    if (bytes_written <= 0)
    {
      if (!retries_left--)
      {
        goto err;
      }
      bool interrupted = vio_should_retry(vio);
      if ((interrupted || bytes_written == 0) &&
        !thr_alarm_in_use(&alarmed))
      {
        thr_alarm(&alarmed, write_timeout, &alarm_buff);
        continue;
      }  
      else
      {
        sql_print_error("Socket write timed out");
        goto err;
      }
    }
    
    pos += bytes_written;
  }
  
  res = 0;
err:
  last_errno = errno;
  error_msg.length(0);
  error_msg.append("Error during write");
  if (thr_alarm_in_use(&alarmed))
  {
    thr_end_alarm(&alarmed);
  }  
  return res;
}

int Tcp_socket::close()
{
  if (vio)
    vio_delete(vio);
  vio = 0;
  inited = 0;
  connected = 0;
  return 0;
}

int Tcp_socket::safe_sleep(uint sec)
{
  if (!sec)
    sec = connect_retry_wait;
    
  return tcp_safe_sleep(current_thd,sec);  
}


int Tcp_socket::ensure_connected()
{
  if (connected)
    return 0;
  
  THD* thd = current_thd;
  
  while (!slave_killed(thd)) 
  {
     if (connect() == 0)
     {
       sql_print_information("Connected to %s:%d", host,port);
       return 0;
     }  
   
     sql_print_error("Error connecting to server %s:%d : %s (%d)",
        host, port, error_msg.c_ptr(),
        last_errno);
     
     close();   
     tcp_safe_sleep(thd,connect_retry_wait);     
     init(-1);
  } 
  
  return 1;
}

static int net_data_is_ready(my_socket fd, int timeout, 
  int event_mask)
{
  SOCKOPT_OPTLEN_TYPE s_err_size = sizeof(uint);
  fd_set sfds,*read_fds = 0, *write_fds=0;
  struct timeval tv;
  time_t start_time, now_time;
  int res, s_err;

  if (fd >= FD_SETSIZE)  
    return 0;
    
  FD_ZERO(&sfds);
  FD_SET(fd, &sfds);
  if (event_mask & POLLIN)
  {
    read_fds = &sfds;
  }
  else
  {
    write_fds = &sfds;
  }
  
  start_time= my_time(0);
  
  for (;;)
  {
    tv.tv_sec = (long) timeout;
    tv.tv_usec = 0;
    if ((res = select(fd+1, read_fds, write_fds, NULL, &tv)) > 0)
      break;
      
    if (res == 0)                                    
    {
      sql_print_error("Socket error: select() returned 0, errno=%d",
        errno); 
      return 0;
    }  
    now_time= my_time(0);
    timeout-= (uint) (now_time - start_time);
    if (errno != EINTR || (int) timeout <= 0)
    {
     sql_print_error("Socket error: select() returned -1, errno=%d",
        errno);
      return 0;
    }  
  }
 s_err=0;
 
 if (getsockopt(fd, SOL_SOCKET, SO_ERROR, (char*) &s_err, &s_err_size) != 0)
 {
    sql_print_error("Socket error: getsockopt failed: %d", errno);
    return 0;
 }
 
 if (s_err)
  {   
    errno = s_err;
    sql_print_error("getsockopt set s_err to %d", errno);
    return 0;
  }
  
  return 1;
}

int Tcp_socket::safe_write(String* buf, uint len)
{
  for (;;)
  {
    if (write(buf,len))
    {
      sql_print_warning("TCP socket write failed, retrying");
      close();
      init(-1);
      
      if (ensure_connected())
      {
        sql_print_error("Could not reconnect after a failed socket write");
        return 1;
      }
    }
    else
      return 0;
  }
}

static int tcp_safe_sleep(THD* thd, int sec)
{
  int nap_time;
  thr_alarm_t alarmed;

  thr_alarm_init(&alarmed);
  time_t start_time= my_time(0);
  time_t end_time= start_time+sec;

  while ((nap_time= (int) (end_time - start_time)) > 0)
  {
    ALARM alarm_buff;
    /*
      The only reason we are asking for alarm is so that
      we will be woken up in case of murder, so if we do not get killed,
      set the alarm so it goes off after we wake up naturally
    */
    thr_alarm(&alarmed, 2 * nap_time, &alarm_buff);
    sleep(nap_time);
    thr_end_alarm(&alarmed);

    if (slave_killed(thd))
      return 1;
    start_time= my_time(0);
  }
  
  return 0;
}

