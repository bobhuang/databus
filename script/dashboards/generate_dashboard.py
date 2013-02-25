#!/usr/bin/env python

import glob
import logging
from optparse import OptionParser, OptionGroup
import os
import os.path
import re
from string import Template
import sys

# Globals

ALL_SOURCES=["anet", "bizfollow", "cappr", "conn", "forum", "fuse", "liar", "mbrrec", "member2", "news", "espresso-ucp"]
ALL_TYPES=["e2e","relay","bst_prod","bst_server"]

#define CLI 
gParser = OptionParser(usage="usage: %prog [options]")
gParser.add_option("-d", "--debug", action="store_true", dest="debug_log", help="debug logging")
gParser.add_option("-O", "--outdir", action="store", type="string", dest="outdir", \
                   default = os.getcwd(), help="debug logging")
gParser.add_option("-s", "--sources", action="store", type="string", dest="sources", default=None, \
                  help="the list of sources whose dashboards to generate: %s"\
                   % list(['all'] + ALL_SOURCES) )
gParser.add_option("-t", "--types", action="store", type="string", dest="types", default='all', \
                  help="the dashboard types to generate: %s" % list(['all'] + ALL_TYPES) ) 
gParser.add_option("-v", "--verbose", action="store_true", dest="verbose_log", help="verbose logging")

def setScriptDir(script_dir):
  global SCRIPT_DIR, TEMPLATE_DIR, SOURCES_TEMPLATE_DIR, RELAYS_TEMPLATE_DIR, BST_PRODUCERS_TEMPLATE_DIR
  global BST_SERVERS_TEMPLATE_DIR, SERVICES_TEMPLATE_DIR
     
  SCRIPT_DIR = script_dir
  TEMPLATE_DIR = os.path.join(SCRIPT_DIR, "templates")
  SOURCES_TEMPLATE_DIR = os.path.join(TEMPLATE_DIR, "sources")
  RELAYS_TEMPLATE_DIR = os.path.join(TEMPLATE_DIR, "relays")
  BST_PRODUCERS_TEMPLATE_DIR = os.path.join(TEMPLATE_DIR, "bst_producers")
  BST_SERVERS_TEMPLATE_DIR = os.path.join(TEMPLATE_DIR, "bst_servers")
  SERVICES_TEMPLATE_DIR = os.path.join(TEMPLATE_DIR, "services")

def exitWithError(msg, printUsage, code):
  sys.stderr.write("%s: %s\n" % (sys.argv[0], msg))
  if printUsage:
    gParser.print_help()
  sys.exit(code)

def createDashboard(template_file, vars, outdir):
  (indir, basename) = os.path.split(template_file)
  (tmpl_name, dashboard_ext) = os.path.splitext(basename)
  fname_templ = Template(tmpl_name)
  dashboard_name = fname_templ.safe_substitute(vars)
  output_name = os.path.join(outdir, dashboard_name)
  logging.info("Creating dashboard %s ..." % output_name)
  with open(output_name, 'w') as templ_out:
    with open(template_file, 'r') as templ_in:
      for l in templ_in:
        line_tmpl = Template(l)
        templ_out.write(line_tmpl.safe_substitute(vars))

def createSourceDashboards(vars, outdir):
  logging.info("Processing source dashboards for source %s ..." % vars["source"])
  source_templates_glob = os.path.join(SOURCES_TEMPLATE_DIR, "*.yaml")
  logging.debug("source templates glob: %s" % source_templates_glob)
  source_templates = glob.glob(source_templates_glob)
  logging.debug("source templates list: %s" % list(source_templates))
  for f in source_templates:
      createDashboard(f, vars, outdir)

def createRelayDashboards(vars, outdir):
  logging.info("Processing relay dashboards for source %s ..." % vars["source"])
  relays_templates_glob = os.path.join(RELAYS_TEMPLATE_DIR, "*.yaml")
  logging.debug("relay templates glob: %s" % relays_templates_glob)
  relay_templates = glob.glob(relays_templates_glob)
  logging.debug("relay templates list: %s" % list(relay_templates))
  for f in relay_templates:
      createDashboard(f, vars, outdir)

def createBstProducerDashboards(vars, outdir):
  logging.info("Processing bootstrap producer dashboards for source %s ..." % vars["source"])
  bst_prod_templates_glob = os.path.join(BST_PRODUCERS_TEMPLATE_DIR, "*.yaml")
  logging.debug("bootstrap producer templates glob: %s" % bst_prod_templates_glob)
  bst_prod_templates = glob.glob(bst_prod_templates_glob)
  logging.debug("bootstrap producer templates list: %s" % list(bst_prod_templates))
  for f in bst_prod_templates:
      createDashboard(f, vars, outdir)

def createBstServerDashboards(vars, outdir):
  logging.info("Processing bootstrap server dashboards for source %s ..." % vars["source"])
  bst_srv_templates_glob = os.path.join(BST_SERVERS_TEMPLATE_DIR, "*.yaml")
  logging.debug("bootstrap server templates glob: %s" % bst_srv_templates_glob)
  bst_srv_templates = glob.glob(bst_srv_templates_glob)
  logging.debug("bootstrap server templates list: %s" % list(bst_srv_templates))
  for f in bst_srv_templates:
      createDashboard(f, vars, outdir)

def createServiceDashboards(vars, outdir):
  logging.info("Processing dashboards for service %s ..." % vars["service"])
  service_templates_glob = os.path.join(BST_SERVERS_TEMPLATE_DIR, "*.yaml")
  logging.debug("service templates glob: %s" % service_templates_glob)
  service_templates = glob.glob(service_templates_glob)
  logging.debug("service templates list: %s" % list(service_templates))
  for f in service_templates:
      createDashboard(f, vars, outdir)

def main(argv):
                    
  # Parse options
  (options, args) = gParser.parse_args()
    
  # debug logging
  log_level = logging.ERROR
  if options.verbose_log:
    log_level = logging.INFO
  elif options.debug_log:
    log_level = logging.DEBUG
    
  logging.basicConfig(level=log_level)
  
  # Process CL options
  if None == options.sources :
    exitWithError("expected sources", True, 2)
  elif "all" == options.sources:
    sourcesList = ALL_SOURCES
  else:
    sourcesList = options.sources.split(",")
  logging.info("sources=%s" % list(sourcesList))
    
  if None == options.types or "all" == options.types:
    typesList = ALL_TYPES
  else:
    typesList = options.types.split(",")
  logging.info("types=%s" % list(typesList))
  
  for s in sourcesList:
    vars=dict(source=s)
    createSourceDashboards(vars, options.outdir)
    
    for t in typesList:
      t = t.lower()
      vars["type"]=t
      if ("relay" == t):
        vars["service"]="databus2-relay-%s" % s
      elif ("bst_prod" == t):
        vars["service"]="databus2-bootstrap-producer-%s" % s
      elif ("bst_server" == t):
        vars["service"]="databus2-bootstrap-server"
      elif ("e2e" == t):
        vars["service"]=None
      else:
        exitWithError(("unknown type: %s" % t), 1, 3)

      createServiceDashboards(vars, options.outdir)

      if ("relay" == t):
        createRelayDashboards(vars, options.outdir)
      elif ("bst_prod" == t):
        createBstProducerDashboards(vars, options.outdir)
      elif ("bst_server" == t):
        createBstServerDashboards(vars, options.outdir)

if __name__ == "__main__" :
  setScriptDir(os.path.dirname(sys.argv[0]))
  
  if len(sys.argv) < 2:
    exitWithError("sources expected", True, 1)
  main(sys.argv[1:])
