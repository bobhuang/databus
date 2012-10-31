#include "dbus_state.h"
#include "my_sys.h"
#include "mysqld.h"
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include "log.h"

int Dbus_state::init()
{
  if (!inited)
  {
    char path[FILENAME_MAX];
    fn_format(path,DBUS_STATE_FNAME,mysql_data_home,"",4+32);
    
    if ((fd = 
         my_open(path,O_RDWR | O_CREAT | O_BINARY, MYF(MY_WME))) < 0)
    {
      sql_print_error("Error opening D-BUS state file %s:(%d)",
        path, errno);
      return 1;  
    }  
    
    char buf[22];
    int bytes_read;
    
    if ((bytes_read = read(fd,buf,sizeof(buf)-1)) <= 0)
    {
      sql_print_information("No info in the D-BUS state file found");
      lsn = 0;
      goto finish;
    }   
    
    // impossible, but take care of safety anyway
    if (bytes_read >= sizeof(buf))
      bytes_read = sizeof(buf) - 1;
      
    buf[bytes_read] = 0;  
    lsn = strtoull(buf,NULL,10);
    sql_print_information("Found stirng %s in rpl_dbus.info file, converted to %llu\n",
      buf, lsn);
  }
  
finish:
  inited = 1;
  return 0;  
}

int Dbus_state::clean()
{
  if (!inited)
    return 0;
  
  flush();  
  ::close(fd);
  fd = -1;
  return 0;
}

int Dbus_state::flush()
{
  if (!inited)
    return 1;
    
  lseek(fd,0,SEEK_SET);
  char buf[22];
  int len = strlen(ullstr(lsn,buf));
  
  if (!len)
    return 1;
    
  if (write(fd,buf,len) != len)
  {
    sql_print_error("Error flushing D-BUS state: %d", errno);
    return 1;
  }
  
  time_t now = time(NULL);
  
  if (now - last_sync_ts > sync_interval)
  {
    fsync(fd);  
    last_sync_ts = now;
  }
  
  
  return 0;
}

