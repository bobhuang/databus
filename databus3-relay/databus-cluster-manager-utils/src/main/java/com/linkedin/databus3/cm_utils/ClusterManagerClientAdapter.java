package com.linkedin.databus3.cm_utils;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.cmclient.NotAReplicatingResourceException;
import com.linkedin.helix.ConfigScope;
import com.linkedin.helix.ConfigScopeBuilder;
import com.linkedin.helix.ExternalViewChangeListener;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.HelixManagerFactory;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.IdealState;

/**
 * An adapter between the client and the RelayClusterManager
 * Provides functionality to instantiate a ClusterManager spectator,
 * listen to external view changes for changes in relays and sources they serve
 *
 */
public class ClusterManagerClientAdapter implements ExternalViewChangeListener,ClusterManagerAdapterInterface
{
  private static Logger LOG = Logger.getLogger(ClusterManagerClientAdapter.class);
  public static final String RPL_DBUS_PORT_CONFIG = "RPL_DBUS_PORT";

  protected HelixManager _clusterManager;
  protected final String _clusterName, _zkConnectString;
  protected boolean _listening = false;

  private ExternalView _externalView;
  public Map<String, ZNRecord> _externalViews;

  /**
   * @param dbName
   * @param zkConnectString
   * @param clusterName
   * @param file
   * @throws ClusterManagerUtilsException
   * @throws Exception
   */
  public ClusterManagerClientAdapter(String clusterName, String zkConnectString)
  {
    _externalViews = new HashMap<String, ZNRecord>(64);
    _clusterName = clusterName;
    _zkConnectString = zkConnectString;

    LOG.info("Creating a ClusterManagerClientAdapter zkConnectString " + _zkConnectString + " on cluster " + _clusterName);
  }

  /**
   * connect to the ZK
   * @throws ClusterManagerUtilsException
   */
  public void connect() throws ClusterManagerUtilsException {
    try
    {
      _clusterManager = HelixManagerFactory.getZKHelixManager(_clusterName, null, InstanceType.SPECTATOR,_zkConnectString);
      _clusterManager.connect();
    }
    catch (Exception e)
    {
      throw new ClusterManagerUtilsException("Failed to connect as spectator to " + _zkConnectString, e);
    }
    LOG.info("Connected to ZK for RplDubsClusterManager: " + _zkConnectString);
  }

  public void disconnect() {
    if(_clusterManager != null)
      _clusterManager.disconnect();
  }

  /**
   * this method needs to be called to start getting the updates
   * @throws ClusterManagerUtilsException
   */
  public void startListeningForUpdates() throws ClusterManagerUtilsException {
    try
    {
      if(!_listening)
        _clusterManager.addExternalViewChangeListener(this);

      _listening = true;
    }
    catch (Exception e)
    {
      throw new ClusterManagerUtilsException("failed to setup listener for  " + _clusterManager, e);
    }
    LOG.info("Started Listening for ZK for ClusterManager: " + _zkConnectString);
  }

  /**
   * For a client to read the external view of the relay cluster
   * This would give the mapping between which (PS,PP,LS) -> Relays
   *
   * cached = false, will fetch it from ZooKeeper
   * cached = true, will return from the cache.
   *
   * @param resourceGroup
   * @return
   */
  @Override
  public ZNRecord getExternalView(boolean cached, String dbName)
  {
    if (cached && _externalView != null)
    {
      return _externalView.getRecord();
    }
    else
    {
      _externalView = _clusterManager.getClusterManagmentTool().getResourceExternalView(_clusterName, dbName);
      if (null == _externalView)
    	  return null;
      else
    	  return _externalView.getRecord();
    }
  }

  /**
  * For a client to read the external view of the relay cluster
  * This would give the mapping between which (PS,PP,LS) -> Relays
  *
  * cached = false, will fetch it from ZooKeeper
  * cached = true, will return from the cache.
  *
  * @param resourceGroup
  * @return
  */
 @Override
 public Map<String, ZNRecord> getExternalViews(boolean cached)
 {
   if (cached && _externalViews != null && !_externalViews.isEmpty()) {
     return _externalViews;
   }
   // re-populate
   _externalViews.clear();
   List<String> dbNames = _clusterManager.getClusterManagmentTool().getResourcesInCluster(_clusterName);
   for(String dbName : dbNames)
   {
     ExternalView externalView = _clusterManager.getClusterManagmentTool().getResourceExternalView(_clusterName, dbName);
     ZNRecord ev = null;
     if (null != externalView)
    	 ev = externalView.getRecord();
     _externalViews.put(dbName, ev);
   }
   return _externalViews;
 }

  /**
   * callback
   */
  @Override
  public synchronized void onExternalViewChange(List<ExternalView> arg0, NotificationContext arg1) {
      if (arg1 == null)
      {
    	  LOG.error("Got onExternalViewChange notification with null context");
    	  return;
      }

      if ( arg1.getType() == NotificationContext.Type.FINALIZE)
      {
          return;
      }
      _externalViews.clear();
      for(Iterator<ExternalView> it=arg0.iterator(); it.hasNext();)
      {
	      ExternalView ev = it.next();
	      ZNRecord zn = ev.getRecord();
	      {
		      // Update the ideal state of relay
		     _externalViews.put(zn.getId(), zn);
        	 LOG.info("Updated external view for dbName " + zn.getId());
	      }
      }
      return;
  }

  public ZNRecord getIdealState(String dbName)
  throws NotAReplicatingResourceException
  {
      IdealState is = _clusterManager.getClusterManagmentTool().getResourceIdealState(_clusterName, dbName);
      if (null == is)
      {
    	  String error = "Both ExternalView and IdealState of the storage cluster are empty. RelayCluster cannot initialize at this point";
    	  LOG.error(error);
    	  return null;
      }
      if (! isAValidResourceIdealState(is))
      {
    	  throw new NotAReplicatingResourceException(dbName);
      }
      return is.getRecord();
  }

  /**
   * return config value for the cluster for a key
   * @param key
   * @return string value
   */
  public String getClusterConfig(String key) {
	LOG.info("Getting configKey for key=" + key);
    ConfigScope cs = new ConfigScopeBuilder().forCluster(_clusterName).build(); // get the config for the cluster
    // get
    Set<String> configs = new HashSet<String>();
    configs.add(key);
    Map<String, String> config_map = _clusterManager.getClusterManagmentTool().getConfig(cs, configs);

    return config_map.get(key);
  }

  /**
   * Write relay configurations ( mysql_port settings for rpldbus )
   *
   * @param storageConfigs
   */
  public void writeRelayConfigs(Map<String, String> relayConfigs)
  {
    LOG.info("Writing relay configs " + relayConfigs.toString());
    ConfigScope cs = new ConfigScopeBuilder().forCluster(_clusterName).build();
    _clusterManager.getClusterManagmentTool().setConfig(cs, relayConfigs);

    Map<String, String> map = new HashMap<String, String>(1);
    ConfigScopeBuilder cfgB = new ConfigScopeBuilder().forCluster(_clusterName);
    for(Map.Entry<String, String> e : relayConfigs.entrySet()) {
      String participant = e.getKey();
      String port = e.getValue();


      cs = cfgB.forParticipant(participant).build();
      map.clear();
      map.put(RPL_DBUS_PORT_CONFIG, port);
      _clusterManager.getClusterManagmentTool().setConfig(cs, map);
    }
    return;
  }

  public boolean isAValidResourceIdealState(IdealState is)
  {
      if (is.getStateModelDefRef().equals("MasterSlave"))
      {
    	  return true;
      }
      else
      {
    	  return false;
      }
  }

}
