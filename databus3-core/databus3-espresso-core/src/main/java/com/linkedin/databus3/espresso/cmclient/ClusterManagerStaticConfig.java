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



/**
 * Static configuration for setup information regarding Helix
 * These parameters are read in as java properties
 * 
 * @author Phanindra Ganti<pganti@linkedin.com>
 */
public class ClusterManagerStaticConfig
{

  private boolean _enabled;	
  private String _relayZkConnectString;
  private String _relayClusterName;
  private String _storageZkConnectString;
  private String _storageClusterName;
  private String _instanceName;
  private String _fileName;

  /**
   * 
   * @param enabled
   * @param relayZkConnectString
   * @param relayClusterName
   * @param storageZkConnectString
   * @param storageClusterName
   * @param instanceName
   * @param fileName
   */
  public ClusterManagerStaticConfig(boolean enabled, String relayZkConnectString, String relayClusterName,
		  							String storageZkConnectString, String storageClusterName,
		  							String instanceName, String fileName)
  {
    super();
    _enabled = enabled;
    _relayZkConnectString = relayZkConnectString;
    _relayClusterName = relayClusterName;
    _storageZkConnectString = storageZkConnectString;
    _storageClusterName = storageClusterName;
    _instanceName = instanceName;
    _fileName = fileName;
  }

  public boolean getEnabled() {
	  return _enabled;
  }
  
  public String getRelayZkConnectString() {
	  return _relayZkConnectString;
  }

  public String getRelayClusterName() {
	  return _relayClusterName;
  }

  public String getStorageZkConnectString() {
	  return _storageZkConnectString;
  }

  public String getStorageClusterName() {
	  return _storageClusterName;
  }

  public String getInstanceName() {
	  return _instanceName;
  }
  
  public String getFileName() {
	  return _fileName;
  }

}
