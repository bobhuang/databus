#!/usr/bin/env python
import datetime
import fnmatch
import glob
import json
import logging
from optparse import OptionParser, OptionGroup
import os
import os.path
import re
import shutil
import subprocess
import sys
import threading

# globals
PARSE_TIMEOUT = datetime.timedelta(minutes=+30)
DEFAULT_FMT = "%(tstamp)s %(severity)s %(machine)s/%(instance)s:%(thread)s [%(ctx)s] %(message)s"

# classes
class LogRecord:
    def __init__(self, service, machine, instance, logfile, lineno, ts, severity, ctx, thread, message):
        self.f = {"service": service, "machine": machine, "instance": instance, "logfile": logfile, "lineno": lineno, \
                  "tstamp": ts, "severity": severity, "ctx": ctx, "thread": thread, "message": message}

class EspressoLogParser:
    def __init__(self, service, machine, instance, logfile):
        self.service = service
        self.machine = machine
        self.instance = instance
        self.logfile = logfile
        self.line_re = re.compile("^([0-9/]+ [0-9:.]+) (TRACE|DEBUG|INFO|WARN|ERROR|CRIT) \[([^]]+)\] \[([^]]+)\] \[[^]]+\] \[\] (.*)")
        pass
    def parse(self, lineno, line):
        m = self.line_re.search(line)
        if None != m:
            d = datetime.datetime.strptime(m.group(1), "%Y/%m/%d %H:%M:%S.%f")
            return LogRecord(self.service, self.machine, self.instance, self.logfile, lineno, d, m.group(2), m.group(3), m.group(4), m.group(5))
        else:
            return None
            
class DatabusLogParser:
    def __init__(self, service, machine, instance, logfile):
        self.service = service
        self.machine = machine
        self.instance = instance
        self.logfile = logfile
        self.line_re = re.compile("^([0-9/]+ [0-9:.]+) (TRACE|DEBUG|INFO|WARN|ERROR|CRIT) \[([^]]+)\] \(([^)]+)\) \+[0-9]+ (.*)")
    def parse(self, lineno, line):
        m = self.line_re.search(line)
        if None != m:
            d = datetime.datetime.strptime(m.group(1), "%Y/%m/%d %H:%M:%S.%f")
            return LogRecord(self.service, self.machine, self.instance, self.logfile, lineno, d, m.group(2), m.group(3), m.group(4), m.group(5))
        else:
            return None

class DefaultLogParser:
    def __init__(self, service, machine, instance, logfile):
        self.service = service
        self.machine = machine
        self.instance = instance
        self.logfile = logfile
        self.line_re = re.compile("^([0-9/]+ [0-9:.]+) (TRACE|DEBUG|INFO|WARN|ERROR|CRIT) \[([^]]+)\] (.*)")
        pass
    def parse(self, lineno, line):
        m = self.line_re.search(line)
        if None != m:
            d = datetime.datetime.strptime(m.group(1), "%Y/%m/%d %H:%M:%S.%f")
            return LogRecord(self.service, self.machine, self.instance, self.logfile, lineno, d, m.group(2), None, None, m.group(3))
        else:
            return None

class LogParserTask(threading.Thread):
    def __init__(self, service, host, instance, logfile, destdir, filters):
        threading.Thread.__init__(self, name="harvest_%s_%s_%s_%s" % (service, host, instance, os.path.basename(logfile)))
        self.service = service
        self.host = host
        self.instance = instance
        self.logfile = logfile
        self.destdir = destdir
        self.filters = filters
        self.databusParser = DatabusLogParser(service, host, instance, logfile)
        self.espressoParser = EspressoLogParser(service, host, instance, logfile)
        self.defaultParser = DefaultLogParser(service, host, instance, logfile)
        self.result = list()
    def run(self):
        logging.info("Parsing %s:%s/%s/%s/logs/%s" % (self.destdir, self.service, self.host, self.instance, self.logfile))
        file_name = os.path.join(self.destdir, self.service, self.host, self.instance, self.logfile)
        if self.logfile.endswith(".gz"):
            logging.info("Unzipping " + self.logfile)
            p = subprocess.Popen(["gunzip", "-c", self.logfile], stdout=subprocess.PIPE)
            if None != p.returncode:
                if 0 != p.returncode:
                    exit_with_error("unable to unzip " + self.logfile, 101)
            f = p.stdout
        else:
            f = open(file_name, 'r') 
        lineno = 0
        for l in f: 
            lineno += 1
            entry = self.databusParser.parse(lineno, l)
            if None == entry:
                entry = self.espressoParser.parse(lineno, l)
            if None == entry:
                entry = self.defaultParser.parse(lineno, l)
            if None == entry:
                logging.debug("unable to parse: " + l)
                continue
            
            if None != self.filters and 0 < len(self.filters):
                match = reduce(lambda x,y: x and y, [f.matches(entry) for f in self.filters])
            else:
                match = True

            if match:
                self.result.append(entry)
            else:
                logging.debug("filter mismatch: " + l)
        logging.info("Done parsing %s:%s/%s/%s/logs/%s" % (self.destdir, self.service, self.host, self.instance, self.logfile))

class LogRecordPrinter:
    def __init__(self, format):
        self.format = format
    def print_rec(self, rec):
        print self.format % rec.f

class RegexRecordFilter:
    def __init__(self, field, filter):
        self.filter = filter
        self.field = field
        self.re = re.compile(filter)
    def matches(self, rec):
        fvalue = rec.f[self.field]
        result = None != self.re.search(fvalue) 
        logging.debug("RegexRecordFilter.matches: %s ~ %s = %s" % (fvalue, self.filter, result))
        return result
    def toString(self):
        return "RegexRecordFilter(%s,%s)" % (self.field, self.filter)

# main()
def main(argv):
    parser = OptionParser(usage="usage: %prog [options] dest_dir", description='''A parallel grep tool for log files from services.
    
    Log files under dest_dir should have the following tree structure: <dest_dir>/<service>/<host>/<instance>/ as generated by harvest_logs.py.
    
    Fields for formats and sorting: service(service name), machine (machine name), 
    instance (instance number),  logfile (log file name), lineno (log record line number),
    tstamp (log record timestamp),
    severity (log record severity),
    ctx (log record context (usually class name)),
        thread -- log record thread
        message -- log record message
    ''')
    parser.add_option("-s", "--services", action="store", type="string", dest="services", default=None, help="A comma-separated list of services whose logs to harvest")
    parser.add_option("-H", "--hosts", action="store", type="string", dest="hosts", default=None, help="A comma-separated list of hosts to harvest; Default: all available hosts for each service")
    parser.add_option("-i", "--instances", action="store", type="string", dest="instances", default=None, help="A comma-separated list of service instances to harvest; Default: all available instances for a each service/host")
    parser.add_option("-L", "--logfiles", action="store", type="string", dest="logfiles", default="*.log", help="A comma-separated list of log files to check; Default: *.log")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose_log", help="verbose logging")
    parser.add_option("-d", "--debug", action="store_true", dest="debug_log", help="debug logging")
    parser.add_option("-e", "--regex", action="append", type="string", dest="regex", default=None, help="regex to match against fields: field:regex")
    parser.add_option("-f", "--format", action="store", type="string", dest="format", default=DEFAULT_FMT, help="output format; Default:" + DEFAULT_FMT)

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
    
    if None == options.services:
        exit_with_error("service list expected ", 1)
        
    if len(args) > 0:
        destdir = args[0]
    else:
        destdir = "." 

    if None != options.instances:
        instances = options.instances.split(',')
    else:
        instances = list()
    if None != options.hosts:
        hosts = options.hosts.split(',')
    else:
        hosts = list()
        
    logging.info("regex=%s" % options.regex)
    filters = list()
    if None != options.regex:
      for r in options.regex:
          parts = r.split(':', 1)
          f = RegexRecordFilter(parts[0], parts[1])
          logging.info("installing filter %s " % (f.toString()))
          filters.append(f)
        
    logfiles = options.logfiles.split(',')
    all_tasks = reduce(lambda l1, l2: l1 + l2, [parse_service_logs(s, hosts, instances, logfiles, destdir, filters) for s in options.services.split(',')])
    logging.debug("Parse tasks: %s " % (all_tasks))
        
    join_start = datetime.datetime.now()
    logging.debug("starting parse tasks %s at %s" % (all_tasks, join_start))
    for task in all_tasks:
        task.start()
        
    join_end = join_start + PARSE_TIMEOUT
    logging.debug("waiting for parse tasks to complete till %s" % join_end)
    for task in all_tasks:
        cur_join_timeout = join_end - datetime.datetime.now()
        cur_join_timeout_sec = cur_join_timeout.seconds + cur_join_timeout.microseconds * 1.0 / 10**6
        logging.debug("joining parse task %s with timeout %s sec" % (task, cur_join_timeout_sec))
        task.join(cur_join_timeout_sec)
        
        if task.isAlive():
            logging.error("parse task %s timeout!")
        else:
            logging.debug("parse task %s complete" % task)
            
    printer = LogRecordPrinter(options.format)
    for task in all_tasks:
        for r in task.result:
            printer.print_rec(r)


def exit_with_error(msg, code):
    sys.stderr.write("harvest_logs:%s\n" %   msg)
    exit(code)

def parse_service_logs(service, hosts, instances, logfiles, destdir, filters):
    if 0 == len(hosts):
        hosts = get_service_hosts(service, destdir)
        logging.debug("hosts discovered for service %s: %s" % (service, hosts))
    result = list()
    for host in hosts:
        tasks = parse_service_host_logs(service, host, instances, logfiles, destdir, filters)
        result += tasks
    return result

def parse_service_host_logs(service, host, instances, logfiles, destdir, filters):
    if 0 == len(instances):
        instances = get_service_host_instances(service, host, destdir)
        logging.debug("instances discovered for serice %s@%s: %s" % (service, host, instances))
    result = reduce(lambda l1, l2: l1 + l2, [parse_service_instance_logs(service, host, i, logfiles, destdir, filters) for i in instances])
    return result
        
def parse_service_instance_logs(service, host, instance, logfiles, destdir, filters):
    files = get_instance_logs(service, host, instance, logfiles, destdir)
    logging.debug("log files for %s@%s/%s: %s" % (service, host, instance, files))
    return [LogParserTask(service, host, instance, l, destdir, filters) for l in files]

def get_service_hosts(service, destdir):
    return get_subdirs_names(os.path.join(destdir, service))

def get_service_host_instances(service, host, destdir):
    return get_subdirs_names(os.path.join(destdir, service, host))

def get_instance_logs(service, host, instance, logfiles, destdir):
    path = os.path.join(destdir, service, host, instance)
    return reduce(lambda d1, d2: d1 + d2, [glob.glob(os.path.join(path, f)) for f in logfiles])

def get_subdirs_names(path):
    return [os.path.basename(d) for d in get_subdirs(path)]        

def get_subdirs(path):
    return [d for d in [os.path.join(path, fname) for fname in os.listdir(path)] if os.path.isdir(d)]


if __name__ == "__main__" :
  main(sys.argv[1:])
