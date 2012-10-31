package com.linkedin.databus3.espresso.cmclient;

public interface IAdapter {

	/**
	 * Add external view change observer 
	 * 
	 * @param DatabusExternalViewChangeObserver Callback instance to be added to observers list
	 */
	public void addExternalViewChangeObservers(StorageExternalViewChangeObserver observer);
	
	/**
	 * Remove external view change observer 
	 * 
	 * @param DatabusExternalViewChangeObserver Callback instance to be removed from observers list
	 */
	public void removeExternalViewChangeObservers(StorageExternalViewChangeObserver observer);

}
