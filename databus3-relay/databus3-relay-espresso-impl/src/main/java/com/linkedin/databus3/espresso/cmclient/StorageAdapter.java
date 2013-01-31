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


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.linkedin.databus3.cm_utils.ClusterManagerClientAdapter;
import com.linkedin.helix.ConfigScopeBuilder;
import com.linkedin.helix.ConfigScope;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.tools.IdealStateCalculatorForStorageNode;

/**
 * An adapter between the StorageNode and the ClusterManager
 * Provides functionality to instantiate a ClusterManager participant, 
 * listen to external view changes
 * 
 * @author Phanindra Ganti<pganti@linkedin.com>
 */
public class StorageAdapter extends ClusterManagerClientAdapter implements IAdapter
{
  protected static Logger LOG = Logger.getLogger(StorageAdapter.class);

  private List<StorageExternalViewChangeObserver> _viewChangeObservers = new ArrayList<StorageExternalViewChangeObserver>();

  public StorageAdapter(String clusterName, String zkConnectString)
  {
	  super(clusterName, zkConnectString);
  }

  /**
   * Obtain the number of partitions from storage cluster
   * 
   * @param dbName
   * @return
   */
  public int getNumPartitions(String dbName)
  throws Exception
  {	 
	  int numPartitions = 0;
	  IdealState is = _clusterManager.getClusterManagmentTool().getResourceIdealState(_clusterName, dbName);
	  if ( null != is)
	      numPartitions  = is.getNumPartitions();
	  else
		  throw new Exception("Database " + dbName + " has probably been removed");
	  LOG.info("Number of partitions on storage node for db " + dbName + " " + numPartitions);	  
	  return numPartitions;
  }

  /**
   * Obtain number of storage nodes in the cluster
   * 
   * @param dbName
   * @return
   */
  public List<String> getStorageNodes()
  throws Exception
  {	  
	  if (null == _clusterManager || !_clusterManager.isConnected())
	  {
		  String errMsg = "ClusterManager is null or not connected. Returning empty list"; 
		  LOG.error(errMsg);
		  throw new Exception(errMsg);
	  }
	  List<String> storageNodes  = _clusterManager.getClusterManagmentTool().getInstancesInCluster(_clusterName);
	  return storageNodes;
  }
  
  public List<String> getDbNames()
  throws Exception
  {
	  if (null == _clusterManager || !_clusterManager.isConnected())
	  {
		  String errMsg = "ClusterManager is null or not connected. Returning empty list"; 
		  LOG.error(errMsg);
		  throw new Exception(errMsg);
	  }
	  List<String> dbNames = _clusterManager.getClusterManagmentTool().getResourcesInCluster(_clusterName);
	  return dbNames;
  }

  /**
   * 
   * @throws Exception
   */
  protected Map<String, String> readStorageConfigs()
  throws Exception
  {
	  Map<String, String> storageNodeMysqlPorts = new HashMap<String, String>();
	  List<String> storageNodes = getStorageNodes();
	  for (String storageNode : storageNodes)
	  {
		 ConfigScope cs = new ConfigScopeBuilder().forCluster(_clusterName).forParticipant(storageNode).build();
		 Set<String> configs = new HashSet<String>();
		 configs.add("mysql_port");
		 configs.add("HELIX_HOST");
		 configs.add("HELIX_PORT");
		 Map<String, String> portMap = _clusterManager.getClusterManagmentTool().getConfig(cs, configs);
		 if (null != portMap)
		 {
			 String portNum = null, snHostname = null, snPortnum = null;
			 if (portMap.containsKey("mysql_port"))
				 portNum = portMap.get("mysql_port");
			 if (portMap.containsKey("HELIX_HOST"))
				 snHostname  = portMap.get("HELIX_HOST");
			 if (portMap.containsKey("HELIX_PORT"))
				 snPortnum  = portMap.get("HELIX_PORT");

			 LOG.info("For storageNode " + storageNode + " HELIX_HOST=" + snHostname + " HELIX_PORT=" + snPortnum + " mysql_port=" + portNum);
			 
			 if ((null != portNum) && (null != snHostname) && (null != snPortnum))
			 {
				 snHostname = snHostname +  "_" + snPortnum; 
				 storageNodeMysqlPorts.put(snHostname, portNum);
			 }
			 else
				 LOG.error("Could not read off storage node mysql settings. May cause errors for REPL_DBUS manager");
		 }
		 else
		 {
			 LOG.error("Error retrieving mysql port number for storage node " + storageNode);
		 }
	  }
	 return storageNodeMysqlPorts;
  }

  static public Map<String, Map<String, String>> getCalculatedIdealState(List<String> relayNodes, List<String> storageNodes, int relayReplicationFactor)
  {	  
	  /* 
	   * Databus notation and Helix are off by 1. 
	   * Relay replication factor = 3 means there are 3 relays connected to a storage node
	   * whereas replicas = 2, means there is one MASTER relay and 2 replicas ( 1+2 = 3 relays in all )
	   */
	  int replicas = relayReplicationFactor - 1;
	  List<String> instanceNames = relayNodes;
	  Collections.sort(instanceNames);
	  int partitions = storageNodes.size();	  
	  String stateUnitGroup = "S";
	  String masterStateValue = "ONLINE";
	  String slaveStateValue = "ONLINE";
	  
	  Collections.sort(storageNodes);
	  Map<String, String> snNamesMap = new TreeMap<String, String>();
	  int count = 0;
	  for (String sn : storageNodes)
	  {
		  String key = "S_" + Integer.toString(count);
		  snNamesMap.put(key, sn);
		  count++;
	  }
	  LOG.info("List of storage nodes " + snNamesMap);
	  LOG.info("List of relay nodes " + relayNodes);
	  
	  ZNRecord zis = IdealStateCalculatorForStorageNode.calculateIdealState(instanceNames, partitions, replicas, stateUnitGroup, masterStateValue, slaveStateValue);
	  zis.setSimpleField("STATE_MODEL_DEF_REF", "OnlineOffline");
	  ZNRecordSerializer zs = new ZNRecordSerializer();
	  LOG.info("Initial IdealState is " + new String(zs.serialize(zis)));

	  Map<String, Map<String, String>> modifiedRisMapFields = new HashMap<String, Map<String, String>>();

	  Map<String, Map<String, String>> risMapFields = zis.getMapFields();
	  for (Iterator<String> it = risMapFields.keySet().iterator(); it.hasNext();)
	  {
		  String key = (String) it.next();
		  if (snNamesMap.containsKey(key)) 
		  {
			  String newKey = snNamesMap.get(key);
			  modifiedRisMapFields.put(newKey, risMapFields.get(key));  
		  }
		  else
		  {
			  LOG.error("Incorrect key");  
		  }
	  }
	  LOG.info("Initial IdealState is " + risMapFields);
	  return modifiedRisMapFields;
  }
  
	@Override
	public void addExternalViewChangeObservers(StorageExternalViewChangeObserver observer) 
	{
		boolean found  = false;
		for (StorageExternalViewChangeObserver obs : _viewChangeObservers)
		{
			if (obs.equals(observer))
			{
				found  = true;
				break;
			}
		}

		if (!found) 
		{
			LOG.info("Add an external view change observer" + observer.getClass().getSimpleName());
			_viewChangeObservers.add(observer);
		}
	}


	@Override
	public void removeExternalViewChangeObservers(StorageExternalViewChangeObserver observer) 
	{
		Iterator<StorageExternalViewChangeObserver> itr = _viewChangeObservers.iterator();

		while (itr.hasNext())
		{
			StorageExternalViewChangeObserver a = itr.next();

			if (a.equals(observer))
			{
				LOG.info("Remove an external view change observer" + observer.getClass().getSimpleName());
				itr.remove();
				break;
			}		  
		}
	}
	
	@Override
	public synchronized void onExternalViewChange(List<ExternalView> arg0, NotificationContext arg1) 
	{
		if (arg1 == null)
		{
			LOG.error("Got onExternalViewChange notification with null context");
			return;
		}

		if ( arg1.getType() == NotificationContext.Type.FINALIZE)
		{
			return;
		}

		LOG.info("Obtained external view change notifications for the following databases" + arg0.size());
		notifyCurrentDatabaseSet(arg0);
		
		for(Iterator<ExternalView> it=arg0.iterator(); it.hasNext();)
		{
			ExternalView ev = it.next();

			LOG.info("Got an external view change notification for dbName " + ev.getId());
			for (StorageExternalViewChangeObserver obs : _viewChangeObservers)
			{
				try 
				{
					obs.onExternalViewChange(ev.getId(), ev);					
				} catch(Exception e)
				{
					LOG.error("Error on invoking external view change on observer for externalView ID" + ev.getId(), e);
				}
				
			}
		}
		return;
	}
	
	/**
	 * Compute the set of all dbNames, and notify the current set of databases to external observers
	 */
	private void notifyCurrentDatabaseSet(List<ExternalView> arg0)
	{
		Set<String> dbNames = new HashSet<String>();
		if (dbNames.size() == 0)
		{
			LOG.info("There are no databases currently on the storage cluster");
		}
		for(Iterator<ExternalView> it=arg0.iterator(); it.hasNext();)
		{
			ExternalView ev = it.next();
			dbNames.add(ev.getId());
		}
		for (StorageExternalViewChangeObserver obs : _viewChangeObservers)
		{
			try 
			{
				obs.handleDatabaseAdditionRemoval(dbNames);
			} catch(Exception e)
			{
				LOG.error("Error on notifying dbNames to external observers" + dbNames, e);
			}			
		}		
	}
			

}
