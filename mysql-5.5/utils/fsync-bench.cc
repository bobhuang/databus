#include <stdio.h>
#include <getopt.h>
#include <sys/types.h>
#include <sys/time.h>
#include <ctype.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "libbench.h"


void usage(const char* error);
void parse_args(int argc, char** argv);
void write_to_file();

long long chunk_size = 16384;
long long file_size = 1024 * 1024 * 256;
const char* file = "file.dat";

static struct option opts[] =
{
  {"file",required_argument,0,'f'},
  {"chunk-size",required_argument,0,'c'},
  {"file-size",required_argument,0,'s'},
  {0,0,0,0}
};

void usage(const char* error)
{
  struct option* o;
  
  if (error)
    fprintf(stderr, "Error: %s\n", error);
    
  fprintf(stderr,"Usage: fsync-bench [args]\nValid arguments:");
  
  for (o = opts; o->name; o++)
  {
    fprintf(stderr,"--%s\n", o->name);
  }
  exit(1);
}



void parse_args(int argc, char** argv)
{
  int c;
  
  for (;;)
  {
    int opt_ind = 0;
    
    c = getopt_long(argc,argv,"f:s:c:",opts,&opt_ind);
    
    if (c == -1)
      return;
      
    switch (c)
    {
      case 'f':
        file = optarg;
        break;
      case 'c':
        if (parse_size(&chunk_size,optarg))
          usage("Invalid --chunk-size argument");
        break;
      case 's':
        if (parse_size(&file_size,optarg))
          usage("Invalid --file-size argument");
        break;
      default:
        usage("Invalid option");      
    }  
  }
}

void write_to_file()
{
  int fd;
  long long bytes_written = 0;
  char* buf;
  Response_stats write_stats, fsync_stats;
  msg("Writing to %s, chunk size = %llu, file size = %llu",
    file, chunk_size, file_size); 

  if (!(buf = (char*)malloc(chunk_size)))
    die("Out of memory");
  
  memset(buf,0xff,chunk_size);
    
  if ((fd = open(file,O_CREAT|O_TRUNC|O_WRONLY,0600)) < 0)
  {
    die("Error %d opening %s", errno, file);
  }
  
  for (; bytes_written < file_size; bytes_written += chunk_size)
  {
    write_stats.start_timer();
    if (write(fd,buf,chunk_size) != chunk_size)
      die("Error during write: %d", errno);
    write_stats.stop_and_record();
    fsync_stats.start_timer();
    fsync(fd);    
    fsync_stats.stop_and_record();
 }
  
  free(buf);
  close(fd);
  
  write_stats.print("Write stats:");
  fsync_stats.print("Fsync stats:");
}

int main(int argc, char** argv)
{
  parse_args(argc,argv);
  write_to_file();
}