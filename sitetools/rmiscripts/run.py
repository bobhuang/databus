#!/usr/bin/env python
import sys;
import os;
import os.path;
import shutil;
sys.path.append('integration-test/script');
import run_base;

if __name__=="__main__":
  print "\n--- Invoking run.py target with arguments ---\n"
  module_name = "rmiscripts"

  shutil.rmtree("rmiservers", ignore_errors=True)
  os.mkdir("rmiservers")
  os.mkdir("rmiservers/codebase")
  os.mkdir("rmiservers/lib")
  os.mkdir("rmiservers/logs")
  os.mkdir("rmiservers/servers")
  os.mkdir("rmiservers/conf")
  shutil.copytree("sitetools/rmiscripts/","rmiservers/bin/")  
  os.chmod("rmiservers/bin/",0755)
  shutil.copy("sitetools/rmiscripts/rmi_conf.sh","rmiservers/conf/")  
