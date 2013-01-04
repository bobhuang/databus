package com.linkedin.databus3.espresso.cmclient;

import java.util.Set;

import com.linkedin.helix.model.ExternalView;

/*
 * Callback Interface for ExternalView changes of relays and other components managed by ClusterManager 
 */

public interface StorageExternalViewChangeObserver
{
	/**
	 * Callback for notifying the new
	 */
    public void handleDatabaseAdditionRemoval(Set<String> dbNames) throws Exception;
	  
	/**
	 * Callback for ExternalViewChange
	 * 
	 * @param dbName : DBName corresponding to the external view
	 * @param oldResourceToServerCoordinatesMap : Previous state of resourceToServerCoordinatesMap
	 * @param oldServerCoordinatesToResourceMap : Previous state of serverToResourceMap
	 * @param newResourceToServerCoordinatesMap : Current state of resourceToServerCoordinatesMap
	 * @param newServerCoordinatesToResourceMap : Current state of serverToResourceMap
	 * @throws Exception 
	 */
	public void onExternalViewChange(String dbName, ExternalView externalView) throws Exception;
}
