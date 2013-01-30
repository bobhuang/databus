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


import org.apache.log4j.Logger;

import com.linkedin.helix.HelixManager;
import com.linkedin.helix.HelixManagerFactory;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.model.Message.MessageType;
import com.linkedin.helix.participant.StateMachineEngine;
import com.linkedin.databus3.espresso.CMResourceListener;

/**
 * An adapter between the Relay and the ClusterManager
 * Provides functionality to instantiate a ClusterManager participant, create a state model and register for messages
 * @author Phanindra Ganti<pganti@linkedin.com>
 */
public class RelayAdapter
{
  private HelixManager _relayClusterManager;

  private final StateMachineEngine _genericStateMachineHandler;
  private final RelayLeaderStandbyStateModelFactory _stateModelFactoryLeaderStandby;
  private final RelayStateModelFactory _stateModelFactoryOnlineOffline;
  private final ResourceManager _rm;
  private final String _file;

  private final String _instanceName;
  /**
   * The resourceGroup node name under the IdealState
   */

  private static Logger LOG = Logger.getLogger(RelayAdapter.class);


  public RelayAdapter(String instanceName, 
                      ClusterParams relayClusterParams, 
                      ClusterParams storageClusterParams, 
                      String file,
                      int relayReplicationFactor
                      ) throws Exception {
    this._instanceName = instanceName;
    _rm = new ResourceManager();
    _file = file;
    if (_file == null)
    {
      _relayClusterManager = 
          HelixManagerFactory.getZKHelixManager(relayClusterParams.getClusterName(), 
                                                instanceName,
                                                InstanceType.PARTICIPANT,
                                                relayClusterParams.getZkConnectString());
    }
    else
    {
    	// No support for file based
    }

    /**
     * Online-Offline State Model / LeaderStandby State Model
     */
    _stateModelFactoryOnlineOffline = 
        new RelayStateModelFactory(relayClusterParams, storageClusterParams, _rm);
    _stateModelFactoryLeaderStandby = 
        new RelayLeaderStandbyStateModelFactory(relayClusterParams, storageClusterParams, relayReplicationFactor, _rm);
    _genericStateMachineHandler = _relayClusterManager.getStateMachineEngine();
    _genericStateMachineHandler.registerStateModelFactory("LeaderStandby", _stateModelFactoryLeaderStandby);
    _genericStateMachineHandler.registerStateModelFactory("OnlineOffline", _stateModelFactoryOnlineOffline);
    
    LOG.info("Creating a RelayAdapter " + _instanceName + " relayClusterParams = " 
        + relayClusterParams.getZkConnectString() + " relayClusterName = " + relayClusterParams.getClusterName()
        + " storageClusterParams = " + storageClusterParams.getZkConnectString() + " storageClusterName = " 
        + storageClusterParams.getClusterName()
        );
  }
  
  /**
   * Invoked during initialization of relay
   * @throws Exception
   */
  public void connect() throws Exception {
    _relayClusterManager.connect();

    
    _relayClusterManager.getMessagingService().registerMessageHandlerFactory(
                                                                             MessageType.STATE_TRANSITION.toString(),
                                                                             _genericStateMachineHandler);

  }

  /**
   * Invoked during shutdown
   * @throws Exception
   */
  public void disconnect()
  throws Exception
  {

	  if (null != _stateModelFactoryLeaderStandby)
	  {
		  LOG.info("Shutting down clustermanager created for relay ideal state generation (OnlineOffline statemodel)");
		  _stateModelFactoryLeaderStandby.disconnect();		  
	  }

	  if ( null != _relayClusterManager)
	  {
		  LOG.info("Shutdown clustermanager created for managing leader election (LeaderStandby statemodel)");
		  _relayClusterManager.disconnect();
	  }
  }
  /**
   * supports one listener
   * will be called on each add/remove resource call
   */
  public void addListener(CMResourceListener listener) {
    if(listener != null)
      _rm.setListener(listener);

  }
}
