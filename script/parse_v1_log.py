#!/usr/bin/env python

from datetime import datetime, date, time
import logging
from optparse import OptionParser, OptionGroup
import os
import re
import subprocess
import sys

def main(argv):
    parser = OptionParser(usage="usage: %prog [options]")
    parser.add_option("-s", "--service", action="store", type="string", dest="service", default=None, help="service to check")
    parser.add_option("-f", "--fabric", action="store", type="string", dest="fabric", default=None, help="fabric to check")
    parser.add_option("-i", "--instances", action="store", type="string", dest="instances", default="i001", help="instances to check")
    parser.add_option("-H", "--hosts", action="store", type="string", dest="hosts", default=None, help="hosts to check")
    parser.add_option("-D", "--date", action="store", type="string", dest="date", default=None, help="date (YYYY-MM-DD) to check")
    parser.add_option("-t", "--tmpdir", action="store", type="string", dest="tmpdir", default=".", help="temp directory to use")
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
    
    if (None == options.service):
      print "service expected"
      return
    
    if (None == options.hosts):
      if (None == options.fabric): 
        print "host or fabric expected"
        return
      res=subprocess.Popen(['glulist', '-c', options.service, '-f', options.fabric],\
                           stdout=subprocess.PIPE).communicate()
      if None != res[1]:
        print "glulist -c %s -f %s error: %s" % (options.service, options.fabric, res[1])
        return
      hosts=res[0].splitlines()
    else:
      hosts=options.hosts.split(",")
      
    instances=options.instances.split(",")
      
    for h in hosts:
      for i in instances:
        try:
          logging.info("Processing %s@%s/%s ..." % (options.service, h, i))
          process_host(options.service, h, i, options.date, options.tmpdir)
        except:
          logging.error("Error processing %s@%s/%s ..." % (options.service, h, i))
  
def process_host(service, host, instance, date, tmpdir):
    dest_dir = "%s/%s/%s-%s" % (tmpdir, service, host, instance)
    if not os.path.isdir(dest_dir):
      logging.info("Creating destination directory: " + dest_dir) 
      os.makedirs(dest_dir)
  
    log_file = get_log_file(service, instance, host, date, dest_dir)
    parse_databus_log(log_file, dest_dir, service, host, instance)

def create_csv_file(fname):
  if os.path.isdir(fname):
    os.rename(fname, fname + ".old")
  return open(fname, "w")

def create_source_csv(source, dest_dir):
  source_query_fname = "%s/%s.csv" % (dest_dir, source)
  fullpath = os.path.abspath(os.path.normpath(source_query_fname))
  of = create_csv_file(source_query_fname)
  
  with open("%s/%s.sql" % (dest_dir, source), "w") as sqlf:
    sqlf.write('''CREATE TABLE IF NOT EXISTS %(source)s (inst VARCHAR(5), host VARCHAR(50), \\
    log_date DATE, log_time TIME, num INT, qry_time INT, read_time INT,\\
   CONSTRAINT PRIMARY KEY (log_time, log_date, inst, host));
CREATE OR REPLACE VIEW %(source)s_by_host AS SELECT HOUR(log_time) hr, log_date, inst, host,   \\
   SUM(num) enum_total, ROUND(SUM(num)/3600.0/count(distinct host), 1) eps, MAX(num) enum_max, \\
   ROUND(AVG(num)) enum_avg, COUNT(num) reqs_num, \\
   ROUND(MAX(1.0 * qry_time / num), 3) qry_max, ROUND(AVG(1.0 * qry_time / num), 3) qry_avg,\\
   ROUND(MAX(1.0 * read_time / num), 3) read_max, ROUND(AVG(1.0 * read_time / num), 3) read_avg,\\
   ROUND(MAX(1.0 * (read_time + qry_time) / num), 3) total_max, \\
   ROUND(AVG(1.0 * (read_time + qry_time) / num), 3) total_avg \\
   FROM %(source)s GROUP BY HOUR(log_time), log_date, inst, host;
CREATE OR REPLACE VIEW %(source)s_by_inst AS SELECT HOUR(log_time) hr, log_date, inst,   \\
   SUM(num) enum_total, ROUND(SUM(num)/3600.0/count(distinct host), 1) eps, MAX(num) enum_max, \\
   ROUND(AVG(num)) enum_avg, COUNT(num) reqs_num, \\
   ROUND(MAX(1.0 * qry_time / num), 3) qry_max, ROUND(AVG(1.0 * qry_time / num), 3) qry_avg,\\
   ROUND(MAX(1.0 * read_time / num), 3) read_max, ROUND(AVG(1.0 * read_time / num), 3) read_avg,\\
   ROUND(MAX(1.0 * (read_time + qry_time) / num), 3) total_max, \\
   ROUND(AVG(1.0 * (read_time + qry_time) / num), 3) total_avg \\
   FROM %(source)s GROUP BY HOUR(log_time), log_date, inst;
CREATE OR REPLACE VIEW %(source)s_by_date AS SELECT HOUR(log_time) hr, log_date,  \\
   SUM(num) enum_total, ROUND(SUM(num)/3600.0/count(distinct host), 1) eps, MAX(num) enum_max, \\
   ROUND(AVG(num)) enum_avg, COUNT(num) reqs_num, \\
   ROUND(MAX(1.0 * qry_time / num), 3) qry_max, ROUND(AVG(1.0 * qry_time / num), 3) qry_avg,\\
   ROUND(MAX(1.0 * read_time / num), 3) read_max, ROUND(AVG(1.0 * read_time / num), 3) read_avg,\\
   ROUND(MAX(1.0 * (read_time + qry_time) / num), 3) total_max, \\
   ROUND(AVG(1.0 * (read_time + qry_time) / num), 3) total_avg \\
   FROM %(source)s GROUP BY HOUR(log_time), log_date;
CREATE OR REPLACE VIEW %(source)s_by_hr AS SELECT HOUR(log_time) hr,  \\
   SUM(num) enum_total, ROUND(SUM(num)/3600.0/count(distinct host), 1) eps, MAX(num) enum_max, \\
   ROUND(AVG(num)) enum_avg, COUNT(num) reqs_num, \\
   ROUND(MAX(1.0 * qry_time / num), 3) qry_max, ROUND(AVG(1.0 * qry_time / num), 3) qry_avg,\\
   ROUND(MAX(1.0 * read_time / num), 3) read_max, ROUND(AVG(1.0 * read_time / num), 3) read_avg,\\
   ROUND(MAX(1.0 * (read_time + qry_time) / num), 3) total_max, \\
   ROUND(AVG(1.0 * (read_time + qry_time) / num), 3) total_avg \\
   FROM %(source)s GROUP BY HOUR(log_time);
LOAD DATA INFILE '%(fullpath)s' IGNORE INTO TABLE %(source)s \\
   FIELDS TERMINATED BY ',' IGNORE 1 LINES\\
   (inst, host, @ldate, @ltime, num, qry_time, read_time) \\
   SET log_date=str_to_date(@ldate, '%%Y/%%m/%%d'),\\
       log_time=str_to_date(CONCAT(@ltime, '000'), '%%H:%%i:%%s.%%f'); 
''' % {"source": source, "fullpath": fullpath});

  of.write("#%s,%s,%s,%s,%s,%s,%s\n" % ("instance", "host", "date", "time", "event_num", \
                                        "qry_time", "read_time"))

  return of

def create_total_csv(service, dest_dir):
  service = service.replace("-databusrelay","")
  total_fname = "%s/%s-totals.csv" % (dest_dir, service)
  fullpath = os.path.abspath(os.path.normpath(total_fname))  
  of = create_csv_file(total_fname)
  
  with open("%s/%s-totals.sql" % (dest_dir, service), "w") as sqlf:
    sqlf.write('''CREATE TABLE IF NOT EXISTS %(service)s_totals (inst VARCHAR(5), host VARCHAR(50), \\
    log_date DATE, log_time TIME, num INT, wt INT, db INT, cb INT, total INT, \\
    CONSTRAINT PRIMARY KEY (log_time, log_date, inst, host));
CREATE OR REPLACE VIEW %(service)s_totals_by_host AS SELECT HOUR(log_time) hr, log_date, inst, host, \\
   SUM(num) enum_total, ROUND(1.0 * SUM(num)/3600/count(distinct host), 1) eps, MAX(num) enum_max, ROUND(AVG(num)) enum_avg,\\
   COUNT(num) reqs_num, ROUND(MAX(1.0 * wt / num), 3) wt_max, ROUND(AVG(1.0 * wt / num), 3) wt_avg,\\
   ROUND(MAX(1.0 * db / num),3) db_max, ROUND(AVG(1.0 * db / num), 3) db_avg, \\
   ROUND(MAX(1.0 * cb / num), 3) cb_max, ROUND(AVG(1.0 * cb / num), 3) cb_avg,\\
   ROUND(MAX(1.0 * total / num), 3) total_max, ROUND(AVG(1.0 * total / num), 3) total_avg\\
   FROM %(service)s_totals GROUP BY HOUR(log_time), log_date, inst, host;
CREATE OR REPLACE VIEW %(service)s_totals_by_inst AS SELECT HOUR(log_time) hr, log_date, inst, \\
   SUM(num) enum_total, ROUND(1.0 * SUM(num)/3600/count(distinct host), 1) eps, MAX(num) enum_max, ROUND(AVG(num)) enum_avg,\\
   COUNT(num) reqs_num, ROUND(MAX(1.0 * wt / num), 3) wt_max, ROUND(AVG(1.0 * wt / num), 3) wt_avg,\\
   ROUND(MAX(1.0 * db / num),3) db_max, ROUND(AVG(1.0 * db / num), 3) db_avg, \\
   ROUND(MAX(1.0 * cb / num), 3) cb_max, ROUND(AVG(1.0 * cb / num), 3) cb_avg,\\
   ROUND(MAX(1.0 * total / num), 3) total_max, ROUND(AVG(1.0 * total / num), 3) total_avg\\
   FROM %(service)s_totals GROUP BY HOUR(log_time), log_date, inst;
CREATE OR REPLACE VIEW %(service)s_totals_by_date AS SELECT HOUR(log_time) hr, log_date,  \\
   SUM(num) enum_total, ROUND(1.0 * SUM(num)/3600/count(distinct host), 1) eps, MAX(num) enum_max, ROUND(AVG(num)) enum_avg,\\
   COUNT(num) reqs_num, ROUND(MAX(1.0 * wt / num), 3) wt_max, ROUND(AVG(1.0 * wt / num), 3) wt_avg,\\
   ROUND(MAX(1.0 * db / num),3) db_max, ROUND(AVG(1.0 * db / num), 3) db_avg, \\
   ROUND(MAX(1.0 * cb / num), 3) cb_max, ROUND(AVG(1.0 * cb / num), 3) cb_avg,\\
   ROUND(MAX(1.0 * total / num), 3) total_max, ROUND(AVG(1.0 * total / num), 3) total_avg\\
   FROM %(service)s_totals GROUP BY HOUR(log_time), log_date;
CREATE OR REPLACE VIEW %(service)s_totals_by_hr AS SELECT HOUR(log_time) hr, \\
   SUM(num) enum_total, ROUND(1.0 * SUM(num)/3600/count(distinct host), 1) eps, MAX(num) enum_max, ROUND(AVG(num)) enum_avg,\\
   COUNT(num) reqs_num, ROUND(MAX(1.0 * wt / num), 3) wt_max, ROUND(AVG(1.0 * wt / num), 3) wt_avg,\\
   ROUND(MAX(1.0 * db / num),3) db_max, ROUND(AVG(1.0 * db / num), 3) db_avg, \\
   ROUND(MAX(1.0 * cb / num), 3) cb_max, ROUND(AVG(1.0 * cb / num), 3) cb_avg,\\
   ROUND(MAX(1.0 * total / num), 3) total_max, ROUND(AVG(1.0 * total / num), 3) total_avg\\
   FROM %(service)s_totals GROUP BY HOUR(log_time);
LOAD DATA INFILE '%(fullpath)s' IGNORE INTO TABLE %(service)s_totals \\
   FIELDS TERMINATED BY ',' IGNORE 1 LINES\\
   (inst, host, @ldate, @ltime, num, wt, db, cb, total) SET log_date=str_to_date(@ldate, '%%Y/%%m/%%d'),\\
   log_time=str_to_date(CONCAT(@ltime, '000'), '%%H:%%i:%%s.%%f'); 
''' % {"fullpath": fullpath, "service": service});

  of.write("#%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % ("instance", "host", "date", "time", "event_num", "wt",\
                                              "db", "cb", "total"))
  return of

def create_scn_csv(service, dest_dir):
  service = service.replace("-databusrelay","")
  scn_fname = "%s/%s-scn.csv" % (dest_dir, service)
  fullpath = os.path.abspath(os.path.normpath(scn_fname))  
  of = create_csv_file(scn_fname)
  
  with open("%s/%s-scn.sql" % (dest_dir, service), "w") as sqlf:
    sqlf.write('''CREATE TABLE IF NOT EXISTS %(service)s_scn (inst VARCHAR(5), host VARCHAR(50), \\
    log_date DATE, log_time TIME, num INT, bop NUMERIC(27,0), eop NUMERIC(27,0), 
    CONSTRAINT PRIMARY KEY (log_time, log_date, inst, host));
CREATE OR REPLACE VIEW %(service)s_scn_by_host AS SELECT HOUR(log_time) hr, log_date, inst, host, \\
   SUM(num) total_events_num, (MAX(eop) - MIN(bop)) / SUM(num) scn_per_upd\\
   FROM %(service)s_scn GROUP BY HOUR(log_time), log_date, inst, host;
CREATE OR REPLACE VIEW %(service)s_scn_by_inst AS SELECT HOUR(log_time) hr, log_date, inst, \\
   SUM(num) total_events_num, (MAX(eop) - MIN(bop)) / SUM(num) scn_per_upd\\
   FROM %(service)s_scn GROUP BY HOUR(log_time), log_date, inst;
CREATE OR REPLACE VIEW %(service)s_scn_by_date AS SELECT HOUR(log_time) hr, log_date, \\
   SUM(num) total_events_num, (MAX(eop) - MIN(bop)) / SUM(num) scn_per_upd\\
   FROM %(service)s_scn GROUP BY HOUR(log_time), log_date;
CREATE OR REPLACE VIEW %(service)s_scn_by_hr AS SELECT HOUR(log_time) hr, \\
   SUM(num) total_events_num, (MAX(eop) - MIN(bop)) / SUM(num) scn_per_upd\\
   FROM %(service)s_scn GROUP BY HOUR(log_time);
LOAD DATA INFILE '%(fullpath)s' IGNORE INTO TABLE %(service)s_scn \\
   FIELDS TERMINATED BY ',' IGNORE 1 LINES\\
   (inst, host, @ldate, @ltime, num, bop, eop) SET log_date=str_to_date(@ldate, '%%Y/%%m/%%d'),\\
   log_time=str_to_date(CONCAT(@ltime, '000'), '%%H:%%i:%%s.%%f'); 
''' % {"fullpath": fullpath, "service": service});

  of.write("#%s,%s,%s,%s,%s,%s,%s\n" % ("instance", "host", "date", "time", "event_num", "bop", "eop"))
  return of


def parse_databus_log(file, dest_dir, service, host, instance):
  source_query_re = re.compile("^([0-9/]+) ([0-9.:]+) DEBUG \[(\w+)\] .*eyTime=([0-9]+)")
  source_summary_re = re.compile("^([0-9/]+) ([0-9.:]+) DEBUG .* source=([^;]*);totalDBTime=([0-9]+);.*;events=([0-9]+)")
  updates_re = re.compile("^([0-9/]+) ([0-9.:]+) INFO .* ([0-9]+) updates => wt:([0-9]+);db:([0-9]+);cb:([0-9]+)=([0-9]+)")
  events_re = re.compile("^events => bop=([0-9]+) eop=([0-9]+) (.*)")
  
  src_qry_files = dict()
  outfiles = []
  totals_file = None
  scn_file = None
  last_date = None
  last_time = None
  last_count = None
  
  try:
    with open(file) as f:
      for line in f:
        logging.debug("in: " + line)
        m = source_query_re.match(line)
        if (None != m):
          logging.debug("found source query line")
          last_src_query = m.group(4)
        else:
          m = source_summary_re.match(line)
          if (None != m):
            logging.debug("found source summary line")
            source = m.group(3)
            of = src_qry_files.get(source)
            if (None == of):
              of = create_source_csv(source, dest_dir)
              outfiles += [of]
              src_qry_files[source] = of
            of.write("%s,%s,%s,%s,%s,%s,%s\n" % (instance, host, m.group(1), m.group(2), \
                                                 m.group(5), last_src_query, m.group(4)))
          else:
            m = updates_re.match(line)
            if (None != m):
              logging.debug("found updates line")
              if (None == totals_file):
                totals_file = create_total_csv(service, dest_dir)
                outfiles += [totals_file]
              last_date = m.group(1)
              last_time = m.group(2)
              last_count = m.group(3)
              totals_file.write("%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % \
                                (instance, host, m.group(1), m.group(2), m.group(3), m.group(4), \
                                 m.group(5), m.group(6), m.group(7)))
            else:
              m = events_re.match(line)
              if (None != m):
                logging.debug("found events line")
                if (None == scn_file):
                  scn_file = create_scn_csv(service, dest_dir)
                  outfiles += [scn_file]
                scn_file.write("%s,%s,%s,%s,%s,%s,%s\n" % \
                               (instance, host, last_date, last_time, last_count, m.group(1), \
                                m.group(2)))
              else:
                logging.debug("unknown line")
  finally:
    for of in outfiles:
      of.close()

def get_log_file(service, instance, host, log_date, dest_dir):
  dir_name = "/export/content/glu/apps/%s/%s/logs" % (service, instance)
  if log_date == None:
    file_name = ("%s_databus.log" % service)
    dest_name = "%s-%s-%s.%s" % (host, instance, file_name, date.today())
    real_dest_name = dest_name
    unpack = 0
  else:
    file_name_base = "%s_databus.log.%s" % (service, log_date)
    file_name = file_name_base + ".gz"
    real_dest_name="%s-%s-%s" % (host, instance, file_name_base)
    dest_name = real_dest_name + ".gz"
    unpack = 1
  src = "%s/%s" % (dir_name, file_name)
  dest = "%s/%s" % (dest_dir, dest_name)
  real_dest = "%s/%s" % (dest_dir, real_dest_name)
  logging.info("reading databus log: %s:%s" % (host,src))
  
  if not os.path.isfile(real_dest):
    liscp(host, src, dest)
    
    if 1 == unpack:
      logging.info("unzipping %s ..." % dest)
      retcode = os.system("gunzip " + dest)
  else:
    logging.info("file already exists")
  
  return real_dest

def liscp(host, src, dest):
  logging.info("liscp %s %s %s" % (host, src, dest))
  retcode = os.system("liscp %s %s %s" % (host, src, dest))
  if 0 != retcode:
    raise RuntimeError("liscp failed: %i" % retcode) 

if __name__ == "__main__" :
  main(sys.argv[1:])
