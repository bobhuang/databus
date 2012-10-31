# 
# Define configuration for all the rmi servers in sh assignment syntax;
# it will be sourced by the rmi[start|status|stop] scripts
#

# codebase_root: the protocol and common path to construct a code base URI for all
#                the servers. 
#   Examples:   file://${RMI_HOME}/codebase/${servername}
#               http://my.hostname:2001/${servername}

#Hardcode values that were otherwise being read in from config/
rmiserver_port=1099

codebase_root=file://${RMI_HOME}/codebase/${servername}

# codebase_dir : the full path to the codebase directory
#   used to discover jar files to be added to the codebase
codebase_dir=${RMI_HOME}/codebase/${servername}

#
# log_dir : directory to write pid files and logs
log_dir=${RMI_HOME}/logs
pid_dir=${log_dir}

# registry_port
registry_port=${rmiservers_port}
