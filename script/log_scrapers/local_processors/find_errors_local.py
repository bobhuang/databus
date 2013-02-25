#!/usr/bin/env python
import glob
import json
import logging
from optparse import OptionParser, OptionGroup
import os
import os.path
import re
import sys

# Globals

service_logs = {}
LOG_LINE_MSG_RE_1 = re.compile("\) \+[0-9]+ (.*)")
LOG_LINE_MSG_RE_2 = re.compile("(ERROR|WARN|CRITICAL) (.*)")

class TextFormatter:
  def print_global_header(self):
    print "================== LOG ERRORS REPORT ====================="
  def print_global_footer(self):
    print "=========================================================="
  def print_service_summary_header(self):
    print "\n\nSUMMARY\n\n" 
    print "| %40s | %15s | %5s | %5s | %5s |" % ("*SERVICE*", "*HOST*", "*CR*", "*ER*", "*WA*")
  def print_service_summary_row(self, service, host, crit_num, error_num, warn_num):
    print "| %40s | %15s | %5s | %5s | %5s |" % (service, host[0:20], crit_num, error_num, warn_num)
  def print_service_summary_footer(self):
    pass
  def print_logs_header(self):
    print "\n\nLOGS\n\n" 
  def print_logs_footer(self):
    pass
  def print_logs_service_header(self, service):
    pass
  def print_logs_service_footer(self, service):
    pass
  def print_logs_host_header(self, service, host):
    pass
  def print_logs_host_footer(self, service, host):
    pass
  def print_logs_logfile_header(self, service, host, logfile):
    print "----------- %s/%s/%s ----------- " % (service, host, logfile)
  def print_logs_logfile_footer(self, service, host, logfile):
    pass
  def print_log_line(self, line_entry):
    print line_entry.line
    if (line_entry.count > 1):
      print "*repeated %i times.*" % line_entry.count

class HtmlFormatter:
  def print_global_header(self):
    print "<h2>LOG ERRORS REPORT</h2>"
  def print_global_footer(self):
    pass
  def print_service_summary_header(self):
    print "<h3>SUMMARY</h3>" 
    print "<table>" 
    print "<tr><th>%s</th><th>%s</th><th>%s</th><th>%s</th><th>%s</th></tr>" % ("*SERVICE*", "*HOST*", "*CR*", "*ER*", "*WA*")
  def print_service_summary_row(self, service, host, crit_num, error_num, warn_num):
    if ".prod" in host:
	row_style="background-color:#ffccff"
    elif host == "*":
	row_style="background-color:silver"
    else:
	row_style=""
    print "<tr style='%s'><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>" % (row_style, service, host[0:20], crit_num, error_num, warn_num)
  def print_service_summary_footer(self):
    print "</table>"
  def print_logs_header(self):
    print "<h3>LOGS</h3>" 
  def print_logs_footer(self):
    pass
  def print_logs_service_header(self, service):
    pass
  def print_logs_service_footer(self, service):
    pass
  def print_logs_host_header(self, service, host):
    pass
  def print_logs_host_footer(self, service, host):
    pass
  def print_logs_logfile_header(self, service, host, logfile):
    print "<h4>%s/%s/%s</h4>" % (service, host, logfile)
    print "<table><tr><th>Count</th><th>Line</th></tr>"
  def print_logs_logfile_footer(self, service, host, logfile):
    print "</table>"
  def print_log_line(self, line_entry):
    print "<tr><td>%i</td><td><code>%s</code></td></tr>" % (line_entry.count, line_entry.line)

def main(argv):
    parser = OptionParser(usage="usage: %prog [options]")
    parser.add_option("-L", "--logdir", action="store", dest="logdir", default=None, help="scraped logs directory")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose_log", help="verbose logging")
    parser.add_option("-d", "--debug", action="store_true", dest="debug_log", help="debug logging")
    parser.add_option("-F", "--format", action="store", dest="format", default=None, help="output format: TXT|HTML")
    
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
    
    if options.logdir == None:
      print "scraped logs directory expected"
      return
      
    out = TextFormatter()
    if options.format == "HTML":
      out = HtmlFormatter()
    elif options.format != "TXT":
      print "Unknown format: " + options.format
      return
    
    for f in glob.glob(options.logdir + "/*-find_errors.log"):
      process_scraped_log(f)
    
    out.print_global_header()
      
    out.print_service_summary_header()
    for svc in service_logs.keys():
      svc_log = service_logs[svc]
      out.print_service_summary_row(svc, '*', svc_log.crit_num, svc_log.error_num, svc_log.warn_num)
      hosts_logs = svc_log.hosts
      if len(hosts_logs) > 0:
        for host in hosts_logs.keys():
          host_logs = hosts_logs[host]
          out.print_service_summary_row('', host, host_logs.crit_num, host_logs.error_num, host_logs.warn_num)
    out.print_service_summary_footer()
    
    out.print_logs_header()
    for svc in service_logs.keys():
      svc_log = service_logs[svc]
      out.print_logs_service_header(svc)
      hosts_logs = svc_log.hosts
      for host in hosts_logs.keys():
        out.print_logs_host_header(svc, host)
        host_log = hosts_logs[host]
        logfiles_logs = host_log.logfiles
        for logfile in logfiles_logs.keys():
          logfile_logs = logfiles_logs[logfile]
          out.print_logs_logfile_header(svc, host, logfile)
          sorted_crit_lines = sorted(logfile_logs.crit_lines.values(), key=LineMapEntry.get_count, reverse=True)
          for l in sorted_crit_lines:
            out.print_log_line(l)
          sorted_err_lines = sorted(logfile_logs.err_lines.values(), key=LineMapEntry.get_count, reverse=True)
          for l in sorted_err_lines:
            out.print_log_line(l)
          sorted_warn_lines = sorted(logfile_logs.warn_lines.values(), key=LineMapEntry.get_count, reverse=True)
          for l in sorted_warn_lines:
            out.print_log_line(l)
          out.print_logs_logfile_footer(svc, host, logfile)
        out.print_logs_host_footer(svc, host)
      out.print_logs_service_footer(svc)
    out.print_logs_footer()
    
    out.print_global_footer()

def get_log_line_msg(line):
  m = LOG_LINE_MSG_RE_1.search(line)
  if m != None:
    return m.group(1)
  else:
    m = LOG_LINE_MSG_RE_2.search(line)
    if m != None:
      return m.group(2)
    else:
      return line

def process_scraped_log(fname):
  logging.info("Processing scraped log " + fname)
  with open(fname, "r") as f:
    for line in f:
      log_obj_arr = json.loads(line)
      for lo in log_obj_arr:
        process_log_obj(lo)

def process_log_obj(log_obj):
  svc_name = log_obj['service']
  hostname = log_obj['hostname']
  logfile = log_obj['logfile']
  
  logging.debug("processing line: svc:%s host:%s file:%s" % (svc_name, hostname, logfile))
  if svc_name in service_logs:
    svc_log = service_logs[svc_name]
  else:
    svc_log = ServiceSummary(svc_name)
    service_logs[svc_name] = svc_log
  svc_log.process_log_obj(log_obj)

class ServiceSummary:
  def __init__(self, svc_name):
    self.service = svc_name
    self.crit_num = 0
    self.error_num = 0
    self.warn_num = 0
    self.hosts = {}
    
  def process_log_obj(self, log_obj):
    crit_num = log_obj['crit_num']
    error_num = log_obj['error_num']
    warn_num = log_obj['warn_num']
    hostname = log_obj['hostname']
    
    self.crit_num += crit_num
    self.error_num += error_num
    self.warn_num += warn_num
  
    if crit_num > 0 or error_num > 0 or warn_num >0:
      if hostname in self.hosts:
        host_log = self.hosts[hostname]
      else:
        host_log = HostSummary()
        self.hosts[hostname] = host_log
      host_log.process_log_obj(log_obj)
  
  def get_host_summary(self, host):
    return self.hosts.get(host)

class HostSummary:
  def __init__(self):
    self.crit_num = 0
    self.error_num = 0
    self.warn_num = 0
    self.logfiles = {}
    
  def process_log_obj(self, log_obj):
    self.crit_num += log_obj['crit_num']
    self.error_num += log_obj['error_num']
    self.warn_num += log_obj['warn_num']
    
    logfile = log_obj['logfile']
    if logfile in self.logfiles:
      logfile_log = self.logfiles[logfile]
    else:
      logfile_log = LogFileSummary()
      self.logfiles[logfile] = logfile_log
    logfile_log.process_log_obj(log_obj)

class LogFileSummary:
  def __init__(self):
    self.crit_lines = {}
    self.err_lines = {}
    self.warn_lines = {}
  def process_log_obj(self, log_obj):
    process_log_line_list(log_obj['crits'], self.crit_lines)
    process_log_line_list(log_obj['errors'], self.err_lines)
    process_log_line_list(log_obj['warnings'], self.warn_lines)

class LineMapEntry:
  def __init__(self, line):
    self.count = 0
    self.line = line
  def inc(self):
    self.count += 1
  def get_count(self):
    return self.count

def process_log_line_list(line_list, out_map):
  for line in line_list:
    msg = get_log_line_msg(line)
    if msg in out_map.keys():
      line_entry = out_map[msg]
    else:
      line_entry = LineMapEntry(line)
      out_map[msg] = line_entry
      
    line_entry.inc()

if __name__ == "__main__" :
  main(sys.argv[1:])
