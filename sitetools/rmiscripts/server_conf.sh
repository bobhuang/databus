# $Id: server_conf.sh 33147 2007-11-18 22:29:12Z dmccutch $ 
# Define configuration for this server in sh assignment syntax;
# it will be sourced by the rmi[start|status|stop] scripts
#
# debug_args : added  to the java commandline if the script is invoked with -d switch
debug_args="${rmiserver.debug_args}"

# vm_args : added to the java commandline every time  the server is started
vm_args="${rmiserver.vm_args}"

# impl : fully package qualified name of class used to start the server
impl=${rmiserver.impl}


