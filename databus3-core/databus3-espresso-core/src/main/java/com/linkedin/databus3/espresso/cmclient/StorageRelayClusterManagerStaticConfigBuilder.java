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
import com.linkedin.databus3.cm_utils.ClusterManagerStaticConfigBaseBuilder;

/**
 * Configuration Builder for setup information regarding Helix
 * 
 * @author Phanindra Ganti<pganti@linkedin.com>
 */

public class StorageRelayClusterManagerStaticConfigBuilder extends ClusterManagerStaticConfigBaseBuilder
			 implements ConfigBuilder<StorageRelayClusterManagerStaticConfig>
{
  public static final int DEFAULT_RELAY_REPLICATION_FACTOR = 1;
  
  private String _storageZkConnectString;
  private String _storageClusterName;
  private int relayReplicationFactor = DEFAULT_RELAY_REPLICATION_FACTOR;
  
  @Override
  public StorageRelayClusterManagerStaticConfig build() throws InvalidConfigException
  {
	  return new StorageRelayClusterManagerStaticConfig(getEnabled(), getEnableDynamic(), getRelayReplicationFactor(),
			  											getVersion(), getRelayZkConnectString(), getRelayClusterName(), 
			  											getStorageZkConnectString(), getStorageClusterName(), 
			  											getInstanceName(), getFileName());
  }
  
  public StorageRelayClusterManagerStaticConfigBuilder()
  {
	  super();
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

  public int getRelayReplicationFactor() {
	return relayReplicationFactor;
  }

  public void setRelayReplicationFactor(int relayReplicationFactor) {
	this.relayReplicationFactor = relayReplicationFactor;
  }
}
