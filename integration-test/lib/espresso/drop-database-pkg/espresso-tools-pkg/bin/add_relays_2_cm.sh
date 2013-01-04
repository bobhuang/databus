#!/bin/sh
#
# A script to set up the cluster and a super-cluster
#


function usage() {
  echo "Usage:$0 [-d] <CLUSTER_MANAGER_ADMIN_HOST_PORT> <CLUSTER_NAME> <RELAY_INSTANCE_NAMES> <SUPER_CLUSTER> "
  echo "  Examples:"
  echo "    $0 localhost:12929 DATABUS_RELAY \"relay1:11140 relay2:11140\" DevClustersV4"
  echo "  Use -d to output commands without executing them (dryrun)."
  exit 1
}

function doCurl()
{
  params=${1}	
  url=${2}
  params=`echo $params | sed s/\'/\"/g`
  cmd="curl -v -d  '$params' -H \"Content-Type: application/json\" $url"
  echo $cmd
  if [ $dryrun -eq 0 ] ; then
    eval $cmd
  fi
}

#
# Main
#

dryrun=0

while getopts d OPT; do
  case "$OPT" in
    d)
      dryrun=1
      shift
      ;;
    \?)
      usage
      shift
      ;;
  esac
done

if test $# -lt 4; then
  echo "ERROR: Missing arguments"
  usage;
fi

CLUSTER_MANAGER_ADMIN_HOST_PORT=$1
CLUSTER_NAME=$2
RELAY_INSTANCE_NAMES=$3
SUPER_CLUSTER=$4

#Create relay cluster 
params="jsonParameters={'command':'addCluster','clusterName':'$CLUSTER_NAME'}"
url="http://$CLUSTER_MANAGER_ADMIN_HOST_PORT/clusters"
doCurl $params $url  

#RELAY_INSTANCE_NAMES
num_relays=`echo $RELAY_INSTANCE_NAMES | wc -w`
for RELAY_INSTANCE_NAME in $RELAY_INSTANCE_NAMES
do
  params="jsonParameters={'command':'addInstance','instanceNames':'$RELAY_INSTANCE_NAME'}"
  url="http://$CLUSTER_MANAGER_ADMIN_HOST_PORT/clusters/$CLUSTER_NAME/instances"
  doCurl $params $url  
done

#now add resource for RelayLeaderElection
  
  params="jsonParameters={'command':'addResource','resourceGroupName':'relayLeaderStandby','partitions':'1','stateModelDefRef':'LeaderStandby'}"
  url="http://$CLUSTER_MANAGER_ADMIN_HOST_PORT/clusters/$CLUSTER_NAME/resourceGroups"
  doCurl $params $url
  params="jsonParameters={'command':'rebalance','replicas':'$num_relays'}"
  url="http://$CLUSTER_MANAGER_ADMIN_HOST_PORT/clusters/$CLUSTER_NAME/resourceGroups/relayLeaderStandby/idealState"
  doCurl $params $url

  #Add the cluster to super cluster
  if [ "X${SUPER_CLUSTER}" != "X" ] ; then
    params="jsonParameters={'command':'enableStorageCluster','grandCluster':'$SUPER_CLUSTER'}"
    url="http://$CLUSTER_MANAGER_ADMIN_HOST_PORT/clusters/$CLUSTER_NAME"
    doCurl $params $url
  fi

exit 0
