#!/usr/bin/env python
'''
  Catch up after running the restore
  This needs to be run in prod env as it needs mysql connection
  Just need to given instance name, figure out mysql port and master from zk
   
'''
__author__ = "David Zhang"
__version__ = "$Revision: 0.1 $"
__date__ = "$Date: 2012/09/27 $"

import sys, os
import pdb
import re
from optparse import OptionParser, OptionGroup
from utility import *
        
def main(argv):
  global options
  global args
  parser = OptionParser(usage="usage: %prog [options]")
  parser.add_option("--cluster_name", action="store", dest="cluster_name", default="ESPRESSO_STORAGE", help="cluster name [default: %default]")
  parser.add_option("--zookeeper_connstr", action="store", dest="zookeeper_connstr", default=None, help="zookeeper connection str [default: %default]")
  parser.add_option("--helix_webapp_host_port", action="store", dest="helix_webapp_host_port", default=None, help="colon separated Host:Port of the helix webapp. [default: %default]")
  parser.add_option("-i","--instance_to_catchup", action="store", dest="instance_to_catchup", default=None, help="Instance to catch up")

  debug_group = OptionGroup(parser, "Debug options", "")
  debug_group.add_option("", "--dry_run", action="store_true", dest="dry_run", default = False, help="dry run mode. [default: %default] ")
  debug_group.add_option("-d", "--debug", action="store_true", dest="debug", default = False,
                     help="debug mode")
  debug_group.add_option("", "--sys_call_debug", action="store_true", dest="enable_sys_call_debug", default = False,
                     help="debug sys call")
  parser.add_option_group(debug_group)
  (options, args) = parser.parse_args()

  if "SCRIPT_DEBUG" in os.environ and os.environ["SCRIPT_DEBUG"].lower() == "true": options.debug = True
  set_debug(options.debug)
  set_sys_call_debug(options.enable_sys_call_debug)
  dbg_print("options = %s  args = %s" % (options, args))
  if not options.instance_to_catchup:
    my_error("Please give instance to catchup")
  if not options.helix_webapp_host_port and not options.zookeeper_connstr:
    my_error("Please give either zookeeper_connstr or helix_webapp_host_port")
  if options.helix_webapp_host_port: set_helix_webapp_host_port(options.helix_webapp_host_port)
  elif options.zookeeper_connstr:  set_zookeeper_conn_str(options.zookeeper_connstr)

  master_slave_instance_id = get_v08_master_instance(options.cluster_name,options.instance_to_catchup,options.zookeeper_connstr)
  if (not master_slave_instance_id):
    my_error("Not able to find a MASTER for instance. Please check the instance name %s" % options.instance_to_catchup)
  (master_instance_id, slave_instance_id) = master_slave_instance_id
  if master_instance_id == slave_instance_id:
    my_error("The given instance id %s is a MASTER. Please give a SLAVE instance" % slave_instance_id)
  master_mysql_port = get_mysql_port(master_instance_id,options.cluster_name,options.zookeeper_connstr)
  slave_mysql_port = get_mysql_port(slave_instance_id,options.cluster_name,options.zookeeper_connstr)

  cmd_to_run = "%s/run_class.sh com.linkedin.espresso.tools.cluster.CatchupAfterBootstrap %s:%s %s:%s" % (this_file_dirname, slave_instance_id.split("_")[0], slave_mysql_port, master_instance_id.split("_")[0], master_mysql_port)
  if options.dry_run:
    print "Dry run: %s" % cmd_to_run
    ret = 0
  else:
    ret = sys_call(cmd_to_run)
  my_exit(ret)

if __name__ == "__main__":
  main(sys.argv[1:])


