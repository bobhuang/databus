package com.linkedin.databus3.espresso.cmclient;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.helix.ConfigScope;
import com.linkedin.helix.ConfigScopeBuilder;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.HelixManagerFactory;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.model.IdealState;

/**
 * An adapter between the StorageNode and helix
 * Provides functionality to instantiate a Helix participant, 
 * listen to external view changes
 * @author Phanindra Ganti<pganti@linkedin.com>
 */
public class RelaySpectator
{
  private static Logger LOG = Logger.getLogger(RelaySpectator.class);

  private HelixManager _clusterManager;
  private String _clusterName, _zkConnectString;
  
  public RelaySpectator(String zkConnectString, String clusterName) 
  throws Exception
  {
	  _clusterName = clusterName;
	  _zkConnectString = zkConnectString;
	  LOG.info("Created RelaySpectator for cluster" + clusterName + " with zkConnectString " + zkConnectString);
  }

  public void connect()
  throws Exception
  {
	  _clusterManager = HelixManagerFactory.getZKHelixManager(_clusterName, null, InstanceType.SPECTATOR, _zkConnectString);
	  _clusterManager.connect();
  }
  
  public void disconnect()
  {
	  LOG.info("Disconnecting Helix manager for RelaySpectator with clusterName " + _clusterName + " " + _zkConnectString);
	  if (null != _clusterManager)
		  _clusterManager.disconnect();
	  _clusterManager = null;
  }
  
  /**
   * Get instances in the cluster
   * 
   * @return
   */
  public List<String> getInstances()
  throws Exception
  {
	  if (null == _clusterManager || !_clusterManager.isConnected())
	  {
		  String errMsg = "ClusterManager is null. Returning empty list"; 
		  LOG.error(errMsg);
		  throw new Exception(errMsg);
	  }
	  List<String> instanceList = _clusterManager.getClusterManagmentTool().getInstancesInCluster(_clusterName);
	  LOG.info("Relay Instances are :" + instanceList);
	  return instanceList;
  }
  
  /**
   * 
   * @param relayClusterName
   * @param zr
   * @param client
   * @throws Exception
   */
  public void writeIdealStateToClusterManager(String dbName, IdealState is)
  throws Exception {
	  if (null == _clusterManager || !_clusterManager.isConnected())
	  {
		  LOG.error("ClusterManager is null or not connected. Cannot write to clusterManager");
		  throw new Exception("ClusterManager is null. Cannot write to clusterManager");
	  }
	  try 
	  {
		  _clusterManager.getClusterManagmentTool().setResourceIdealState(_clusterName, dbName, is);
	  } catch (Exception e)
	  {
		  LOG.info("Exception trying to write ideal state to cluster manager", e);
		  throw e;		  
	  }
	  return;
  }

  /**
   * Write storage node configurations ( mysql_port settings for storage node )
   * 
   * @param dbName
   * @param storageConfigs
   */
  public void writeStorageNodeConfigs(Map<String, String> storageConfigs)
  throws Exception
  {
	  if (null == _clusterManager || !_clusterManager.isConnected())
	  {
		  String errMsg = "Could not write storage node configs as clusterManager is null or is not connected";
		  LOG.error("Could not write storage node configs as clusterManager is null");
		  throw new Exception(errMsg);
	  }
	  LOG.info("Writing storage node configs " + storageConfigs.toString());
	  ConfigScope cs = new ConfigScopeBuilder().forCluster(_clusterName).build();
	  _clusterManager.getClusterManagmentTool().setConfig(cs, storageConfigs);
	  return;
  }

  public void writeNumPartitions(String dbName, int numPartitions)
  throws Exception
  {
	  if (null == _clusterManager || !_clusterManager.isConnected())
	  {
		  String errMsg = "Could not write number of partitions on storage cluster. ClusterManager is null or is not connected";
		  LOG.error(errMsg);
		  throw new Exception(errMsg);
	  }
	  LOG.info("Writing number of partitions on storage cluster to relay cluster " + numPartitions);
	  ConfigScope cs = new ConfigScopeBuilder().forCluster(_clusterName).build();
	  Map<String, String> partitionMap = new HashMap<String, String>();
	  partitionMap.put(dbName, Integer.toString(numPartitions));
	  _clusterManager.getClusterManagmentTool().setConfig(cs, partitionMap);
	  return;
  }
  
  /**
   * Removes dbName (resourceGroup ) from the cluster
   *  
   * @param dbName
   * @throws Exception
   */
  public void removeResourceGroup(String dbName)
  throws Exception
  {
	  if (null == _clusterManager || !_clusterManager.isConnected())
	  {
		  String errMsg = "Could not remove resourceGroup " + dbName + ". ClusterManager is null or is not connected";
		  LOG.error(errMsg);
		  throw new Exception(errMsg);
	  }
	  _clusterManager.getClusterManagmentTool().dropResource(_clusterName, dbName);
  }

}
