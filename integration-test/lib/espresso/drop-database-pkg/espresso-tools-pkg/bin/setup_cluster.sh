#!/bin/sh
#
# A script to set up the cluster and a super-cluster
#

function usage() {
  echo "Usage:$0 [-d] <CLUSTER_MANAGER_ADMIN_HOST_PORT> <CLUSTER_NAME> <ESPRESSO_INSTANCE_NAMES> <MASTER_SLAVE_RESOURCES> <SCHEMA_ROOT_DIR> <SUPER_CLUSTER> <CLUSTER_MANAGER_INSTANCE_NAMES>"
  echo "  Specify \"\" for SUPER_CLUSTER if it is not used"
  echo "  Examples:"
  echo "    $0 localhost:12929 ESPRESSO_FOOBAR \"storagenode1:12918 storagenode2:12918\" \"db1,1024,3 db2,2048,5\" ../../../../schemas_registry supercluster_v4 clustermanagernode:12928"
  echo "    The last two parameters can be specified as \"\""
  echo "  To add a cluster to an existing super-cluster, use the same command but ignore the error that says super-cluster already exists"
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

if test $# -ne 7; then
  echo "ERROR: Incorrect number of arguments"
  usage;
fi

CLUSTER_MANAGER_ADMIN_HOST_PORT=$1
CLUSTER_NAME=$2
ESPRESSO_INSTANCE_NAMES=$3
MASTER_SLAVE_RESOURCES=$4
SCHEMA_ROOT_DIR=$5
SUPER_CLUSTER=$6
CLUSTER_MANAGER_INSTANCE_NAMES=$7

if [ ! -d ${SCHEMA_ROOT_DIR}/schemata ] ; then
  echo Directory ${SCHEMA_ROOT_DIR}/schemata not found.
  exit 1
fi

# Ensure that the number of elements in ESPRESSO_INSTANCE_NAMES is at least as much as the number of replicas requested in each MASTER_SLAVE_RESOURCES.
# and the number of partitions matches that in schema declaration.
n_esp_instances=`echo $ESPRESSO_INSTANCE_NAMES | wc -w`;
for resource in $MASTER_SLAVE_RESOURCES
do
  resource_name=`echo $resource | cut -f1 -d,`
  num_partitions=`echo $resource | cut -f2 -d,`
  num_replicas=`echo $resource | cut -f3 -d,`
  if [ `expr $n_esp_instances` -lt `expr $num_replicas` ] ; then
    echo Resource $resource_name cannot have more replicas than number of instances in cluster "($n_esp_instances)"
    exit 1
  fi
  schema_num_partitions=`./schema-parser.sh -D ${resource_name} -P numBuckets -S ${SCHEMA_ROOT_DIR}`
  if [ $? -ne 0 ] ; then
    echo "Cannot fetch schema for DB " ${resource_name}
    exit 1
  fi
  if [ $schema_num_partitions -ne $num_partitions ] ; then
    echo "Specified number of partitions($num_partitions) does not match the number in schema($schema_num_partitions) for $resource_name"
    exit 1
  fi
done

#Create storage cluster
params="jsonParameters={'command':'addCluster','clusterName':'$CLUSTER_NAME'}"
url="http://$CLUSTER_MANAGER_ADMIN_HOST_PORT/clusters"
doCurl $params $url

#ESPRESSO_INSTANCE_NAMES
for ESPRESSO_INSTANCE_NAME in $ESPRESSO_INSTANCE_NAMES
do
  params="jsonParameters={'command':'addInstance','instanceNames':'$ESPRESSO_INSTANCE_NAME'}"
  url="http://$CLUSTER_MANAGER_ADMIN_HOST_PORT/clusters/$CLUSTER_NAME/instances"
  doCurl $params $url
done

#Add the databases to the cluster
for RESOURCE in $MASTER_SLAVE_RESOURCES
do
  resource_name=`echo $RESOURCE | cut -f1 -d,`
  num_partitions=`echo $RESOURCE | cut -f2 -d,`
  num_replicas=`echo $RESOURCE | cut -f3 -d,`

  params="jsonParameters={'command':'addResourceGroup','resourceGroupName':'$resource_name','partitions':'$num_partitions','stateModelDefRef':'MasterSlave'}"
  url="http://$CLUSTER_MANAGER_ADMIN_HOST_PORT/clusters/$CLUSTER_NAME/resourceGroups"
  doCurl $params $url

  params="jsonParameters={'command':'rebalance','replicas':'$num_replicas'}"
  url="http://$CLUSTER_MANAGER_ADMIN_HOST_PORT/clusters/$CLUSTER_NAME/resourceGroups/$resource_name/idealState"
  doCurl $params $url

done	

#create the super cluster if specified
if [ "X${SUPER_CLUSTER}" != "X" ] ; then
  params="jsonParameters={'command':'addCluster','clusterName':'$SUPER_CLUSTER'}"
  url="http://$CLUSTER_MANAGER_ADMIN_HOST_PORT/clusters"
  doCurl $params $url

  # Add cluster manager instances to this super cluster
  for CM_INSTANCE_NAME in $CLUSTER_MANAGER_INSTANCE_NAMES
  do
    params="jsonParameters={'command':'addInstance','instanceNames':'$CM_INSTANCE_NAME'}"
    url="http://$CLUSTER_MANAGER_ADMIN_HOST_PORT/clusters/$SUPER_CLUSTER/instances"
    doCurl $params $url	
  done

  #Add the cluster to super cluster
  params="jsonParameters={'command':'enableStorageCluster','grandCluster':'$SUPER_CLUSTER'}"
  url="http://$CLUSTER_MANAGER_ADMIN_HOST_PORT/clusters/$CLUSTER_NAME"
  doCurl $params $url
fi
exit 1
