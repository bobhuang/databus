#!/usr/bin/env python
from datetime import datetime, date, time
import fnmatch
import glob
import json
import logging
from optparse import OptionParser, OptionGroup
import os
import os.path
import re
import socket
import sys

# Global Constants
GLU_SERVICES_DIR = "/export/content/glu/apps"
GLU_SERVICE_DIR_PATTERN = GLU_SERVICES_DIR + "/%s"
GLU_SERVICE_LOGS_DIR_PATTERN = GLU_SERVICE_DIR_PATTERN + "/*/logs"
HOSTNAME=socket.gethostname()

def main(argv):
    parser = OptionParser(usage="usage: %prog [options]")
    parser.add_option("-s", "--services", action="store", type="string", dest="services", default=None, help="List of services to check")
    parser.add_option("-L", "--logfiles", action="store", type="string", dest="logfiles", default=None, help="List of log files to check")
    parser.add_option("-J", "--json", action="store_true", dest="to_json", help="print result in JSON")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose_log", help="verbose logging")
    parser.add_option("-d", "--debug", action="store_true", dest="debug_log", help="debug logging")
    
    # Parse options
    (options, args) = parser.parse_args()
    #print ("options: %s args: %s" % (options, args))
    
    # debug logging
    log_level = logging.ERROR
    if options.verbose_log:
      log_level = logging.INFO
    elif options.debug_log:
      log_level = logging.DEBUG
    
    logging.basicConfig(level=log_level)
    
    # Figure out services to check
    if options.services == None:
      options.services = get_services()
    else:
      options.services = options.services.split(",")
    logging.debug("Using services: %s" % options.services)
    
    # Figure out logs files to check
    if options.logfiles == None:
      options.logfiles = get_logfiles(options.services)
    else:
      options.logfiles = set(options.logfiles.split(","))    
    logging.debug("Using log files: %s" % options.logfiles)
    
    result = check_logfiles(options.services, options.logfiles)
    if options.to_json:
      print json.dumps(result)
    else:
      print result

def check_logfiles(services, logfiles):
  result = []
  now = datetime.now()
  now_str = now.strftime("%Y%m%d-%H%M%S")
  for s in services:
    result += check_service_logfiles(s, logfiles, now_str)
  return result

def check_service_logfiles(service, logfiles, now_str):
  result = []
  service_logs_dir = GLU_SERVICE_LOGS_DIR_PATTERN % service
  for f in glob.glob(service_logs_dir + "/*"):
    if os.path.basename(f) in logfiles:
      result.append(check_service_log(service, f))
  return result

def check_service_log(service, logfile):
  #print "check_service_log(%s, %s)" % (service, logfile)
  crit_num = 0
  error_num = 0
  warn_num = 0
  crit_lines = []
  error_lines = []
  warn_lines = []
  crit_re = re.compile(" CRIT")
  error_re = re.compile(" ERROR ")
  warn_re = re.compile(" WARN")
  with open(logfile, "r") as f:
    for line in f:
      if crit_re.search(line):
        crit_num += 1
        crit_lines.append(line.rstrip())
      elif error_re.search(line):
        error_num += 1
        error_lines.append(line.rstrip())
      elif warn_re.search(line):
        warn_num += 1
        warn_lines.append(line.rstrip())
  return {"service":service, "logfile": os.path.basename(logfile), "hostname": HOSTNAME,\
          "crit_num": crit_num, "error_num": error_num, "warn_num": warn_num,\
          "crits": crit_lines, "errors": error_lines, "warnings":warn_lines}

def get_service_logfiles(service):
  log_files = glob.glob((GLU_SERVICE_LOGS_DIR_PATTERN + "/*databus*.log") % service)
  log_filenames = [os.path.basename(x) for x in log_files if x.find("configuration") == -1]
  logging.debug(".log files: %s" % log_filenames)
  
  #out_files = glob.glob((GLU_SERVICE_LOGS_DIR_PATTERN + "/*databus*.out") % service)
  #out_filenames = [os.path.basename(x) for x in out_files ]
  #logging.debug(".out files: %s" % out_filenames)
  
  #return set(log_filenames) | set(out_filenames)
  return set(log_filenames)

def get_logfiles(services):
  logfiles = set()
  for s in services:
    logfiles |= get_service_logfiles(s)
  return logfiles


def get_services():
  service_dirs = glob.glob(GLU_SERVICES_DIR + "/*")
  service_names = [os.path.basename(x) for x in service_dirs]
  return service_names

if __name__ == "__main__" :
  main(sys.argv[1:])
