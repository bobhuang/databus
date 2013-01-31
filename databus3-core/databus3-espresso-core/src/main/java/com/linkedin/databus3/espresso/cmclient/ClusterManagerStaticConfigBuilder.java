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


import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;

/**
 * Configuration Builder for setup information regarding Helix
 * 
 * @author Phanindra Ganti<pganti@linkedin.com>
 */
public class ClusterManagerStaticConfigBuilder
       implements ConfigBuilder<ClusterManagerStaticConfig>
{

  private boolean _enabled = false;	
  private String _relayZkConnectString = null;
  private String _relayClusterName = null;
  private String _storageZkConnectString = null;
  private String _storageClusterName = null;
  private String _instanceName = null;
  private String _fileName = null;
  
  @Override
  public ClusterManagerStaticConfig build() throws InvalidConfigException
  {
	  return new ClusterManagerStaticConfig(_enabled, _relayZkConnectString, _relayClusterName, 
			  								_storageZkConnectString, _storageClusterName, 
			  								_instanceName, _fileName);
  }
  
  public boolean getEnabled() {
	return _enabled;
  }

  public void setEnabled(boolean enabled) {
	_enabled = enabled;
  }

  public String getRelayZkConnectString() {
	  return _relayZkConnectString;
  }

  public void setRelayZkConnectString(String relayZkConnectString) {
	  _relayZkConnectString = relayZkConnectString;
  }

  public String getRelayClusterName() {
	  return _relayClusterName;
  }

  public void setRelayClusterName(String relayClusterName) {
	  _relayClusterName = relayClusterName;
  }

  public String getStorageZkConnectString() {
	  return _storageZkConnectString;
  }

  public void setStorageZkConnectString(String storageZkConnectString) {
	  _storageZkConnectString = storageZkConnectString;
  }

  public String getStorageClusterName() {
	  return _storageClusterName;
  }

  public void setStorageClusterName(String storageClusterName) {
	  _storageClusterName = storageClusterName;
  }

  public String getInstanceName() {
	  return _instanceName;
  }

  public void setInstanceName(String instanceName) {
	  _instanceName = instanceName;
  }  
  public String getFileName() {
	  return _fileName;
  }

  public void setFileName(String fileName) {
	  _fileName = fileName;
  }  

}
