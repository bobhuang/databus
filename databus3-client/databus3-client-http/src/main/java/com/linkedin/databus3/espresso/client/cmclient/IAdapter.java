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


import java.util.List;
import java.util.Map;

import com.linkedin.databus.client.pub.DatabusServerCoordinates;
import com.linkedin.databus.core.cmclient.ResourceKey;
import com.linkedin.databus2.core.DatabusException;

public interface IAdapter {
	
	public Map<ResourceKey, List<DatabusServerCoordinates>> getExternalView(boolean cached, String dbName);

	/**
	 * Returns a map between a relay and the subscriptions it hosts ( described by ResourceKey ).
	 * 
	 * @param : cached , use cached znRecord if it already exists
	 * @return
	 */
	public Map<DatabusServerCoordinates, List<ResourceKey>> getInverseExternalView(boolean cached, String dbName);
	
	/**
	 * Add external view change observer 
	 * 
	 * @param DatabusExternalViewChangeObserver Callback instance to be added to observers list
	 */
	public void addExternalViewChangeObservers(DatabusExternalViewChangeObserver observer)
	throws DatabusException;
	
	/**
	 * Remove external view change observer 
	 * 
	 * @param DatabusExternalViewChangeObserver Callback instance to be removed from observers list
	 */
	public void removeExternalViewChangeObservers(DatabusExternalViewChangeObserver observer);
	
	/**
	 * Returns number of partitions in the cluster
	 * 
	 * @param dbName 
	 */
	public int getNumPartitions(String dbName);

}
