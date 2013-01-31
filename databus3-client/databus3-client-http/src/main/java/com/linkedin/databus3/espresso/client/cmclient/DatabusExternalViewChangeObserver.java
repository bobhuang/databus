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

/*
 * Callback Interface for ExternalView changes of relays and other components managed by ClusterManager 
 */
public interface DatabusExternalViewChangeObserver 
{
	/**
	 * Callback for ExternalViewChange
	 * 
	 * @param dbName : DBName corresponding to the external view
	 * @param oldResourceToServerCoordinatesMap : Previous state of resourceToServerCoordinatesMap
	 * @param oldServerCoordinatesToResourceMap : Previous state of serverToResourceMap
	 * @param newResourceToServerCoordinatesMap : Current state of resourceToServerCoordinatesMap
	 * @param newServerCoordinatesToResourceMap : Current state of serverToResourceMap
	 */
	public void onExternalViewChange(String dbName, 
									 Map<ResourceKey, List<DatabusServerCoordinates>> oldResourceToServerCoordinatesMap, 
									 Map<DatabusServerCoordinates, List<ResourceKey>> oldServerCoordinatesToResourceMap,
									 Map<ResourceKey, List<DatabusServerCoordinates>> newResourceToServerCoordinatesMap,
									 Map<DatabusServerCoordinates, List<ResourceKey>> newServerCoordinatesToResourceMap);
}
