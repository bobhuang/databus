#!/usr/bin/env python2.6
import os; execfile('/usr/local/linkedin/bin/activate_this.py', dict(__file__='/usr/local/linkedin/bin/activate_this.py')); del os

import datetime
import fnmatch
import glob
import json
import linkedin.tools.fabric
import logging
from optparse import OptionParser, OptionGroup
import os
import os.path
import shutil
import subprocess
import sys
import threading

# Global Constants
HARVEST_TIMEOUT = datetime.timedelta(minutes=+30)
HOST_OS_MAP = dict()

# Classes
class LogHarvestThread(threading.Thread):
    def __init__(self, service, host, instance, logfiles, destdir):
        threading.Thread.__init__(self, name="harvest_%s_%s_%s" % (service, host, instance))
        self.service = service
        self.host = host
        self.instance = instance
        self.logfiles = logfiles
        self.destdir = destdir
    
    def run(self):
        logging.info("Harvesting %s:/export/content/glu/apps/%s/%s/logs/%s" % (self.host, self.service, self.instance, self.logfiles))
        if not os.access(self.destdir, os.F_OK):
            os.makedirs(self.destdir)
            
        remote_rsync_path=""
        if HOST_OS_MAP[self.host] == "SunOS":
            remote_rsync_path="--rsync-path /usr/local/bin/rsync"
        
        src_list = " ".join(map(lambda x: "%s:/export/content/glu/apps/%s/%s/logs/%s" % (self.host, self.service, self.instance, x), self.logfiles))
        rsync_cmd = "/usr/bin/rsync -zut %s %s %s" % (remote_rsync_path, src_list, self.destdir)
        logging.debug("rsync command: %s" % rsync_cmd)
        rsync_code = os.system(rsync_cmd)

def exit_with_error(msg, code):
    sys.stderr.write("harvest_logs:%s\n" %   msg)
    exit(code)

# main()
def main(argv):
    parser = OptionParser(usage="usage: %prog [options] dest_dir", description="A parallel sync/harvest tool for log files from services. Files are stored under <dest_dir>/<service>/<host>/<instance>/.")
    parser.add_option("-s", "--services", action="store", type="string", dest="services", default=None, help="A comma-separated list of services whose logs to harvest")
    parser.add_option("-f", "--fabric", action="store", type="string", dest="fabric", default=None, help="A fabric to harvest")
    parser.add_option("-H", "--hosts", action="store", type="string", dest="hosts", default=None, help="A comma-separated list of hosts to harvest")
    parser.add_option("-t", "--tag", action="store", type="string", dest="tag", default=None, help="the cluster tag to use")
    parser.add_option("-i", "--instances", action="store", type="string", dest="instances", default="i001", help="A comma-separated list of service instances to harvest; Default: i001")
    parser.add_option("-L", "--logfiles", action="store", type="string", dest="logfiles", default="*.log", help="A comma-separated list of log files to check; Default: *.log")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose_log", help="verbose logging")
    parser.add_option("-d", "--debug", action="store_true", dest="debug_log", help="debug logging")
    parser.add_option("-c", "--clean_dest", action="store_true", dest="clean_dest", help="clean up contents of destination directory before syncing, i.e. copy files")

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
    instances = options.instances.split(',')
    logfiles = options.logfiles.split(',')
    
    all_tasks = list()
    for service in options.services.split(','):
        if None == options.fabric:
            if None == options.hosts:
                exit_with_error("either hosts or fabric have to be specified", 2)
            else:
                hosts=options.hosts.split(',')
        else:
            hosts = get_service_hosts(service, options.fabric, options.tag)
        tasks = harvest_service(service, hosts, instances, logfiles, destdir, options.clean_dest)
        all_tasks += tasks
    join_start = datetime.datetime.now()
    
    logging.debug("starting harvest tasks %s at %s" % (all_tasks, join_start))
    for task in all_tasks:
        task.start()
        
    join_end = join_start + HARVEST_TIMEOUT
    logging.debug("waiting for harvest tasks to complete till %s" % join_end)
    for task in all_tasks:
        cur_join_timeout = join_end - datetime.datetime.now()
        cur_join_timeout_sec = cur_join_timeout.seconds + cur_join_timeout.microseconds * 1.0 / 10**6
        logging.debug("joining harvest task %s with timeout %s sec" % (task, cur_join_timeout_sec))
        task.join(cur_join_timeout_sec)
        
        if task.isAlive():
            logging.error("harvest task %s timeout!")
        else:
            logging.debug("harvest task %s complete" % task)
        
def harvest_service(service, hosts, instances, logfiles, destdir, clean_dest):
    global HOST_OS_MAP
    
    task_list = list()
    for host in hosts:
        #figure out which are the SunOS hosts since they need special rsync invocation
        if not HOST_OS_MAP.has_key(host):
            guess_os_cmd="ssh -tt %s 'uname' | grep SunOS" % (host)
            guess_os_retcode = os.system(guess_os_cmd)
            if 0 == guess_os_retcode:
                HOST_OS_MAP[host]="SunOS"
            else:
                HOST_OS_MAP[host]="Linux"
            logging.debug("host OS discovery %s --> %s" % (host, HOST_OS_MAP[host]))
            
        for instance in instances:
            dest_path = os.path.join(destdir, service, host, instance)
            if os.access(dest_path, os.F_OK) and clean_dest:
                shutil.rmtree(dest_path)
            
            logging.debug("harvesting %s for service %s at host %s/%s to %s" % (logfiles, service, hosts, instances, dest_path))
            new_task = LogHarvestThread(service, host, instance, logfiles, dest_path)
            task_list.append(new_task)
    return task_list

'''
def get_service_hosts(service, fabric, tag):
    logging.debug("getting host list for %s/%s" % (fabric, service))
    if None == tag:
        glulist_proc = subprocess.Popen(['/usr/local/linkedin/bin/glulist', '-s', service, '-f', fabric], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    else:
        logging.debug("using tag %s" % (tag))
        glulist_proc = subprocess.Popen(['/usr/local/linkedin/bin/glulist', '-t', tag, '-f', fabric], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    glulist_res = glulist_proc.communicate(5000)
    if glulist_proc.returncode != 0:
        exit_with_error("unable to read host list for %s/%s: %s\n%s" % (fabric, service, glulist_res[0], glulist_res[1]), 10)
    hosts = glulist_res[0].splitlines()
    logging.debug("host list for %s/%s = %s" % (fabric, service, hosts))
    return hosts 
'''

def get_service_hosts(service, fabric, tag):
    logging.debug("getting host list for %s/%s" % (fabric, service))
    f = linkedin.tools.fabric.Fabric(fabric, system_filter=None, use_live=False)
    if None == tag:
        hosts = [h.name for p in f.products for h in p.hosts_for_service(service)]
    else:
        hosts = [h.name for p in f.products for h in p.hosts_for_tag(tag)]
    logging.debug("host list for %s/%s = %s" % (fabric, service, hosts))
    return hosts
 
if __name__ == "__main__" :
  main(sys.argv[1:])
