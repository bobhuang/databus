# Sets up parameters to specify which database to drop, and which cluster to drop from
source setup_env.inc

HME=$(pwd)

# This is an advisory for respective Espresso clients to stop traffic for respective databases
echo "This script assumes that the traffic has been stopped for the database being dropped"

# Drop schemas
#${HME}/espresso-tools-pkg/bin/schema-tool.sh --src zk://${ZK_HOST}:${ZK_PORT}/schemas_registry/${STORAGE_CLUSTER_NAME} --db-name ${DB_NAME} --delete 
${HME}/espresso-tools-pkg/bin/schema-tool.sh --src http://${ROUTER_HOST}:${ROUTER_PORT} --db-name ${DB_NAME} --delete 

# Drop the database
#${HME}/helix-core-pkg/bin/helix-admin --dropResource ${STORAGE_CLUSTER_NAME} ${DB_NAME} --zkSvr ${ZK_HOST}:${ZK_PORT}
curl -i -X DELETE http://${HELIX_CONTROLLER_HOST}:${HELIX_CONTROLLER_PORT}/clusters/${STORAGE_CLUSTER_NAME}/resourceGroups/${DB_NAME}
sleep 30

# Remove pending external view resource groups from the storage cluster
${HME}/zk-tools-pkg/bin/zkCli.sh -server ${ZK_HOST}:${ZK_PORT} delete /${STORAGE_CLUSTER_NAME}/EXTERNALVIEW/${DB_NAME}
sleep 30

maxNumInstances=${MAX_NUM_INSTANCES}
# Figure out the set of all relay machines, and remove the persistent files
for h in `cat $HME/cluster.hosts`
do
   for i in `seq 1 $maxNumInstances`
       do
       dataDir=/export/content/data/databus3-relay-espresso/i00$i/mmap
       for files in `ssh $h sudo -u app ls $dataDir/metaFile.${DB_NAME}_*`
       do
          echo "Print all metaInfoFiles" $files
          for sessionDir in `ssh $h sudo -u app cat $files | grep "sessionId" | gawk '{print $2}'`
          do
              echo "Removing sessionDir " $sessionDir  
              ssh $h sudo -u app rm -rf $dataDir/$sessionDir   
          done
          echo "Removing metaInfoFiles" $files
          ssh $h sudo -u app rm -rf $files
       done
   done
done


# Restart the clients
echo "Please restart the client machines, to remove any events from the delete database"
