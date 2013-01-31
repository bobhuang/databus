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


import java.text.ParseException;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.cmclient.ResourceKey;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.relay.AddRemovePartitionInterface;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.SourceIdNameRegistry;
import com.linkedin.databus3.espresso.CMResourceListener;
import com.linkedin.databus3.espresso.EspressoRelay.StaticConfig;
import com.linkedin.databus3.espresso.schema.EspressoBackedSchemaRegistryService;

public class ClusterManagerPhysicalSourceConfigBuilder extends BasePhysicalSourceConfigBuilder
implements CMResourceListener
{
	private final Logger LOG = Logger.getLogger(ClusterManagerPhysicalSourceConfigBuilder.class.getName());
	private final StaticConfig _staticConfig;
	private final AddRemovePartitionInterface _relay;
	private boolean _enabled; // if true - accept all the changes
	private RelayAdapter _relayAdapter;
	
	public ClusterManagerPhysicalSourceConfigBuilder(StaticConfig staticConfig, 
	                                                 SourceIdNameRegistry sourceIdNameRegistry, 
	                                                 EspressoBackedSchemaRegistryService ess,
	                                                 AddRemovePartitionInterface relay) {
		super();
		_staticConfig = staticConfig;
		_sourceIdNameRegistry = sourceIdNameRegistry;
		_schemaRegistryService = ess;
		_relay = relay;
		_enabled = true;
		_relayAdapter = null;
	}
	
	/**
	 * enable/disable add/remove
	 * @param enable
	 */
	public void enableAddRemoveResource(boolean enable){
	  _enabled = enable;
	}

	public void connectToCM() throws InvalidConfigException {

	  StorageRelayClusterManagerStaticConfig cmsc = _staticConfig.getClusterManager();
    if (! cmsc.getEnabled()) {
      throw new InvalidConfigException("Trying to connect to CM when CM is not enabled");
    }

    /** 
     * ClusterManager Integration is enabled
     * Perform error checking on the parameters and connect as a clustermanager client
     */ 
    final String instanceName     = cmsc.getInstanceName();
    final ClusterParams relayClusterParams = new ClusterParams(cmsc.getRelayClusterName(), cmsc.getRelayZkConnectString());
    /**
     * TODO (DBUSDDS-404)
     * we need to get a storageCluster params relevant to this specific DB
     */
    final ClusterParams storageClusterParams = new ClusterParams(cmsc.getStorageClusterName(), cmsc.getStorageZkConnectString());
    final String file             = cmsc.getFileName();
    LOG.info("relayZkConnectString = " + relayClusterParams.getZkConnectString() + " relayClusterName = " 
        + relayClusterParams.getClusterName() + " instancename = " + instanceName + " file = " + file);

    try {
    	_relayAdapter = 
    			new RelayAdapter(instanceName, relayClusterParams, storageClusterParams, file, cmsc.getRelayReplicationFactor());
    	_relayAdapter.addListener(this); // listener will be called on add/remove calls
    	_relayAdapter.connect();
    } catch (Exception e) {
    	LOG.error("Exception while connecting to CM ", e);
    }
	}

	public void disconnectFromCM() 
	{
		try {
			_relayAdapter.disconnect();
		} catch (Exception e) {
			LOG.error("Exception while disconnecting from CM ", e);
		}
	}

	@Override
	public void addResource(String rs) throws ParseException, DatabusException
	{
	  if(!_enabled) {
	    LOG.warn("Ignoring addResource call (disabled): " + rs);
	  }
	  ResourceKey rk;
	  PhysicalSourceConfig psc = null;
	  rk = new ResourceKey(rs);
	  psc = buildOnePSSCFromResourceKey(rk);
	  psc.setResourceKey(rs);
	  PhysicalSourceStaticConfig pssc = psc.build();
	  _relay.addPartition(pssc);
	  LOG.info("ADDING buffer for : " + rs + "=>" + pssc);
	}

	@Override
	public void removeResource(String rs) throws ParseException, DatabusException
	{
	  if(!_enabled) {
	    LOG.warn("Ignoring removeResource call (disabled): " + rs);
      }
	  ResourceKey rk;
	  PhysicalSourceConfig psc = null;
	  rk = new ResourceKey(rs);
	  psc = buildOnePSSCFromResourceKey(rk);
	  psc.setResourceKey(rs);
	  PhysicalSourceStaticConfig pssc = psc.build();
    _relay.removePartition(pssc);
	  LOG.info("REMOVING buffer for : " + rs + "=>" + pssc);
	}
	
	@Override
	public void dropDatabase(String dbName) throws DatabusException
	{
		if(!_enabled) {
			LOG.warn("Ignoring removeResource call (disabled): " + dbName);
		}
		_relay.dropDatabase(dbName);
	}
}
