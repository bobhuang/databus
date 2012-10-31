# Sets up parameters to specify which database to drop, and which cluster to drop from
source setup_env.inc

fullFileName=`readlink -f $0`
HME=`dirname ${fullFileName}`

# This is an advisory for respective Espresso clients to stop traffic for respective databases
echo "Ensure that the traffic has been stopped for your Database"

# Drop schemas
$HME/espresso-tools-pkg/bin/schema-tool.sh --src ${ROUTER_HOST}:${ROUTER_PORT} --db-name ${DB_NAME} --delete 

# Drop the database
$HME/helix-core-pkg/bin/helix-admin --dropResource ${STORAGE_CLUSTER_NAME} ${DB_NAME} --zkSvr ${ZK_HOST}:${ZK_PORT}
sleep 30

# Remove pending external view resource groups from the storage cluster
$HME/zk-tools-pkg/bin/zkCli.sh -server ${ZK_HOST}:${ZK_PORT} delete /${STORAGE_CLUSTER_NAME}/EXTERNALVIEW/${DB_NAME}
sleep 30

maxNumInstances=${MAX_NUM_INSTANCES}
# Figure out the set of all relay machines, and remove the persistent files
for h in `cat $HME/cluster.hosts`
do
   for i in `seq 1 $maxNumInstances`
       do
       dataDir = /export/content/data/databus3-relay-espresso/i00$i/mmap
       for files in `ssh $h ls $dataDir/metaFile.${DB_NAME}_*`
       do
          echo "print metaInfoFiles" $files
          for sessionDir in `cat $files | grep "sessionId" | gawk '{print $2}'`
          do
              echo "print sessionDir" $sessionDir 
          done
          ssh $h sudo -u app rm -rf $dataDir/$sessionDir   
          ssh $h sudo -u app rm -rf $files
       done
   done
done


# Restart the clients
echo "Please restart the client machines, to remove any events from the delete database"
