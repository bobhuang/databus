#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdarg.h>
#include <mysql.h>
#include <getopt.h>
#include "libbench.h"


static const char* host = "localhost";
static int port = 3306;
static const char* user = "root";
static const char* pw = "";
static const char* db = 0;
static const char* sock = 0;
static const char* table = 0;
static const char* col = 0;
static const char* where = 0;
static MYSQL* glob_mysql = 0;

static struct option opts[] =
{
  {"host",required_argument, 0, 'h'},
  {"user",required_argument, 0, 'u'},
  {"password",required_argument, 0, 'p'},
  {"db",required_argument, 0, 'D'},
  {"socket",required_argument, 0, 'S'},
  {"port",required_argument, 0, 'P'},
  {"table",required_argument, 0, 't'},
  {"column",required_argument, 0, 'c'},
  {"where",required_argument, 0, 'w'},
  {0,0,0,0}
};

static void parse_args(int argc,char** argv);
static void usage();
static void glob_db_connect();

static void parse_args(int argc,char** argv)
{
  int c;
  
  for (;;)
  {
    int opt_ind = 0;
    c = getopt_long(argc,argv,
       "h:u:D:P:p:S:t:c:w:",
              opts,&opt_ind);  
    
    if (c == -1)
      return;
      
    switch (c)
    {
      case 'h':
        host = optarg;
        break;
      case 'u':
        user = optarg;
        break;
      case 'D':
        db = optarg;
        break;
      case 'S':
        sock = optarg;
        break;
      case 't':
        table = optarg;
        break;
      case 'p':
        pw = optarg;
        break;
      case 'P':
        port = atoi(optarg);
        break;
      case 'w':
        where = optarg;
        break;  
      case 'c':
        col = optarg;
        break;  
      default:
        usage();     
    }  
  }
  
  if (!db || !table || !column)
    die("You must speficify --db , --table, and --column");
}

void usage()
{
  die("Usage: tokenizer [arguments]");
}

static void glob_db_connect()
{
  glob_mysql = mysql_init(0);
  
  if (!glob_mysql)
    die("Out of memory");
    
  if (!mysql_real_connect(glob_mysql,host,user,pw,NULL,port,sock,
    CLIENT_MULTI_STATEMENTS))
  {
    die("Error connecting to MySQL: %s", mysql_error(glob_mysql));
  }
}

void process_data()
{
  char where_buf[4096];
  
  if (!where)
    where_buf[0] = 0;
  else
    snprintf(where_buf, sizeof(where_buf), " where %s", where);  
    
  MYSQL_RES* res = safe_queryf(glob_mysql, 
    " select %s from %s %s", column, table, where);
       
   
}

int main(int argc, char** argv)
{
  parse_args(argc,argv);
  glob_db_connect();
  process_data();
  mysql_close(glob_mysql);
  return 0;
}