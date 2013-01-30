package com.linkedin.databus3.espresso.client.cmclient;
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


import java.util.Map;
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.cmclient.ResourceKey;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus3.espresso.cmclient.RelayClusterManagerStaticConfig;
import com.linkedin.databus.client.pub.DatabusServerCoordinates;

public class RelayClusterInfoProvider 
{
	private IAdapter _ca;
	
	private static Logger LOG = Logger.getLogger(RelayClusterInfoProvider.class);
	
	/**
	 * Provides a connection to the Relay Cluster from where we can read off the external 
	 * view of the relay cluster and provides information on which 
	 * @param dbName
	 * @param config
	 * @throws Exception
	 */
	public RelayClusterInfoProvider(RelayClusterManagerStaticConfig config)
	throws DatabusException
	{
		try 
		{
			_ca = new ClientAdapter(config.getRelayZkConnectString(), config.getRelayClusterName(), 
					(String)null, config.getEnableDynamic());
		}
		catch (Exception e)
		{
			LOG.info("Error making a connection to RelayClusterManager ", e);
			throw new DatabusException(e.getMessage());
		}
	}

	/**
	 * 
	 * @throws DatabusException
	 */
	public RelayClusterInfoProvider(int testcaseNum)
	throws DatabusException
	{
		try 
		{
			_ca = new MockClientAdapter(testcaseNum);
		}
		catch (Exception e)
		{
			LOG.info("Error making a connection to RelayClusterManager ", e);
			throw new DatabusException(e.getMessage());
		}
	}

	/**
	 * Returns a map between a relay and the subscriptions it hosts ( described by ResourceKey ).
	 * 
	 * @return
	 */
	public Map<DatabusServerCoordinates, List<ResourceKey>> getInverseExternalView(String dbName)
	{
		return _ca.getInverseExternalView(true, dbName);
	}
	
	/**
	 * Returns mapping between a resource and set of all relays that are serving it
	 * 
	 * @return
	 */
	public Map<ResourceKey, List<DatabusServerCoordinates>> getExternalView(String dbName)
	{		
		return _ca.getExternalView(true, dbName);
	}
	
	
	public void registerExternalViewChangeObservers(DatabusExternalViewChangeObserver observer)
	throws DatabusException
	{
		_ca.addExternalViewChangeObservers(observer);
	}
	
	public void deregisterExternalViewChangeObservers(DatabusExternalViewChangeObserver observer)
	{
		_ca.removeExternalViewChangeObservers(observer);
	}
	
	public int getNumPartitions(String dbName)
	{
		return _ca.getNumPartitions(dbName);
	}
}
