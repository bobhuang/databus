#ifndef LI_SOCKET_H
#define LI_SOCKET_H

int li_socket_listen();
int li_socket_close();

#define LI_SOCKET_BUF_SIZE 16384
#define LI_SOCKET_OK_LEN  4
#define LI_SOCKET_ERR_LEN  4
#define LI_SOCKET_GET_RESP_HEADER_SIZE (2 /*code*/ + 4 /* packet size */ + \
  4 /* num_records */ + 8 /* position */ + 1 /* flags */)
#define LI_SOCKET_GET_REQ_HEADER_SIZE (1 /*mode*/ + 8 /* position */ + \
  4 /* num_records */)

#define LI_SOCKET_RECORD_BUFFER_PAD (16*1024*1024)

#define LI_SOCKET_POS_LOOKUP 0x1
#define LI_SOCKET_SIZE_IN_RECORDS 0x2
#define LI_SOCKET_SKIP_FIRST_RECORD 0x4

#define LI_SOCKET_EOF  0x1

enum
{
  LI_SOCKET_CMD_LOAD=0,
  LI_SOCKET_CMD_QUIT=1,
  LI_SOCKET_CMD_GET=2
};

enum 
{
  LI_SOCKET_ERR_OP=1,
  LI_SOCKET_ERR_FATAL=2
};

#define LI_SOCKET_ERR_MSG_SIZE 256
#define LI_SOCKET_PORT 22357

struct Li_socket_conn
{
  uint id;
  int fd;
  struct sockaddr_in addr;
  Tcp_socket* sock;
  uchar* packet;
  uint packet_len;
#ifdef MYSQL_SERVER  
  THD* thd;
  bool thd_is_mine;
#endif  
  char err_msg[LI_SOCKET_ERR_MSG_SIZE];
  uchar* buf;
  char* table_name, *db_name;
  uint db_len,table_len;
  uchar* buf_end;
  TABLE_LIST tbl;
  uchar* table_len_pos; 
  uchar* record_buffer;
  uint record_buffer_size;
  uchar* record_buffer_ptr;
  
  Li_socket_conn():id(0),fd(-1),sock(0),packet(0),packet_len(0),thd(0),
    thd_is_mine(1),buf(0),record_buffer(0),record_buffer_size(0),
    record_buffer_ptr(0)
  {
    err_msg[0] = 0;
    
    if ((packet = (uchar*)my_malloc(LI_SOCKET_BUF_SIZE,MYF(0))))
    {
      packet_len = LI_SOCKET_BUF_SIZE;
    }
    init_thd();
  }
  
  // this one is for replication SQL slave thread
  Li_socket_conn(THD* thd):id(0),fd(-1),sock(0),packet(0),
    packet_len(0),thd(thd),thd_is_mine(0),buf(0),record_buffer(0),
    record_buffer_size(0),
    record_buffer_ptr(0)
  {
    err_msg[0] = 0;
  }
  
  bool is_open() { return sock != 0; }
  
  void set_read_timeout(uint t)
  {
    sock->set_read_timeout(t);
  }
  
  void set_write_timeout(uint t)
  {
    sock->set_write_timeout(t);
  }
  
  int init_thd()
  {
#ifdef MYSQL_SERVER    
    if (thd) return 0;
    
    thd = new THD;
    
    if (!thd) return 1;
    
    if (thd->store_globals())
    {
      delete thd;
      thd = 0;
      return 1;
    }
#endif    
    return 0;
  }
  
  ~Li_socket_conn()
  {
#ifdef MYSQL_SERVER    
    if (thd && thd_is_mine)
    {  
      delete thd;
      thd = 0;
    }  
#endif    
    if (packet)
    {  
      my_free(packet);
      packet = 0;
    }  
    
    if (record_buffer)
    {
      my_free(record_buffer);
      record_buffer = 0;
    }
  }
  
  void close();
  int read(uchar* buf, uint len);
  int write(uchar* buf, uint len);
  int send_error(uint16 error_code, const char* msg);
  int send_ok();
  int handle_load(uchar* buf, uint len);
  int handle_get(uchar* buf, uint len);
  int parse_table_and_open(uint len); // helper method
  int buffer_current_record();
  int secure_record_buffer(uint buf_size);
  uint guess_rec_size();
  void store_position(uchar* pos, uint len);
  char* get_err_msg() { return err_msg; }
};


#endif