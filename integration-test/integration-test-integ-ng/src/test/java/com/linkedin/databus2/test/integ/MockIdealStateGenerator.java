package com.linkedin.databus2.test.integ;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;

public class MockIdealStateGenerator
{
	
	final String MODULE = MockIdealStateGenerator.class.getName();
	final Logger LOG = Logger.getLogger(MODULE);
	
  /**
   * generate an ideal state for espresso use-case locally
   * i.e., it generates locally ( does not use IdealStateGeneratorForStorageNode, does not use storage node
   * external view
   */	
  public ZNRecord espressoRelayIdealStateGenerator(int numPartitionsPerEspressoDB, int replicationFactor,
		  									List<String> relayNodes, List<String> storageNodes, 
		  									String physicalPartitionName, 
		  									String logicalPartitionName,
		  									String espressoDBName)
  {
	  final int numRelays = relayNodes.size();
	  final int numNodes = storageNodes.size();
	  double npn = Math.ceil((1.0 * replicationFactor * numPartitionsPerEspressoDB) / numNodes);
	  final int numPartitionsPerNode = (int)npn;
	  
	  /// Relay configuration
	  Map<String, String> map = new HashMap<String, String>();
	  List<String> list = new LinkedList<String>();
	  for (int r=0; r< numRelays; ++r) {
		  map.put(relayNodes.get(r), "ONLINE");		
		  list.add(relayNodes.get(r));
	  }
	  
	  /// Storage configuration
	  int num_partitions_assigned_per_node = 0;
	  int cur_node_id = 0;

	  ZNRecord record = new ZNRecord(espressoDBName);
	  record.setSimpleField("NUM_PARTITIONS",     Integer.toString(numPartitionsPerEspressoDB));
	  record.setSimpleField("STATE_MODEL_DEF_REF","OnlineOffline");
	  record.setSimpleField("IDEAL_STATE_MODE","CUSTOMIZED");
	  record.setSimpleField("REPLICAS", Integer.toString(replicationFactor));

	  ZNRecordSerializer serializer = new ZNRecordSerializer();

	  for (int j = 1; j <= replicationFactor; j++)
	  {
		  for (int i = 1; i <= numPartitionsPerEspressoDB; i++)
		  {
			  if (num_partitions_assigned_per_node == numPartitionsPerNode){
				  cur_node_id++;
				  num_partitions_assigned_per_node = 0;
			  }
			  String key = storageNodes.get(cur_node_id);
			  key += "," +  physicalPartitionName + "," + String.format(logicalPartitionName, i);
			  if (j == 1)
				  key += ",MASTER";
			  else
				  key += ",SLAVE";			  
			  num_partitions_assigned_per_node++;

			  record.getMapFields().put(key, map);
		  }
	  }
	  LOG.info(new String(serializer.serialize(record)));
	  return record;
  }
	
  /**
   * This is for integration testing only
   * @return
   */
  public ZNRecord obtainRelayIdealState() 
  {
	  /**
	   *  partition, physical node information
	   * NUM_PARTITIONS = Total number of logical partitions per logical source
	   */ 
	  final int NUM_PARTITIONS_PER_ESPRESSO_DB = 64;
	  final int REPLICATION_FACTOR = 1;
	  final int SCHEMA_VERSION = 1;
	  
	  /**
	   *  Physical Database machines 
	   */
	  final List<String> storageNodes = new ArrayList<String>(
			  Arrays.asList(
					  "esv4-app74.stg.linkedin.com_12918",
					  "esv4-app75.stg.linkedin.com_12918",					  
					  "esv4-app76.stg.linkedin.com_12918",
					  "esv4-app77.stg.linkedin.com_12918"
			  ));

	  /**
	   * Physical Databases hosted
	   */
	  final String espressoDBName = "BizProfile";
	  final String physicalPartitionName = espressoDBName;

	  /**
	   * Logical Partition numbers ( same as physical partition numbers for espresso case)
	   */
	  final String logicalPartitionName  = "p%d_" + SCHEMA_VERSION;
	  
	  /**
	   *  Physical Relay machines
	   */
	  final List<String> relayNodes = new ArrayList<String>(
			  Arrays.asList(
			  "esv4-app81.stg.linkedin.com_8887"
			  ));
	  	  
	  ZNRecord zn = espressoRelayIdealStateGenerator(
			  NUM_PARTITIONS_PER_ESPRESSO_DB, REPLICATION_FACTOR, 
			  relayNodes, storageNodes, 
			  physicalPartitionName, logicalPartitionName, espressoDBName);
	  return zn;
  }
}
