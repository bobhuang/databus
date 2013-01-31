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
 * Static configuration for setup information regarding relay cluster
 * These parameters are read in as java properties
 * 
 * This is used by databus client which requires access to the relay cluster's external view only
 * 
 * @author Phanindra Ganti<pganti@linkedin.com>
 */
public class StorageRelayClusterManagerStaticConfig extends ClusterManagerStaticConfigBase
{
	private final String _storageZkConnectString;
	private final String _storageClusterName;
	private final int _relayReplicationFactor;
	
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
	public StorageRelayClusterManagerStaticConfig(boolean enabled, boolean enableDynamic, int relayReplicationFactor,
												  String version, String relayZkConnectString, String relayClusterName,
												  String storageZkConnectString, String storageClusterName,
												  String instanceName, String fileName)
	{
		super(enabled, enableDynamic, version, relayZkConnectString, relayClusterName, instanceName, fileName);
		_storageZkConnectString = storageZkConnectString;
		_storageClusterName = storageClusterName;
		_relayReplicationFactor = relayReplicationFactor;
	}
	
	public String getStorageZkConnectString() {
		return _storageZkConnectString;
	}
	
	public String getStorageClusterName() {
		return _storageClusterName;		
	}

	public int getRelayReplicationFactor() {
		return _relayReplicationFactor;
	}	
}
