package com.linkedin.databus3.espresso.cmclient;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.databus3.espresso.CMResourceListener;

/**
 * Main class for managing resources as defined in the cluster manager
 * Provides a thread-safe way of adding / removing resources as per callbacks from Clustermanager
 * Provides an api to empty current set of resources to another buffer and dispatched for processing 
 *
 * @author Phanindra Ganti<pganti@linkedin.com>
 */
public class ResourceManager
{
	private static Logger logger = Logger.getLogger(ResourceManager.class);
	
	private CMResourceListener _rsListener = null;
		  
	public ResourceManager()
	{
	}

	/**
	 * listener to add/remove resource(partition)
	 * @param listener
	 */
	public void setListener(CMResourceListener listener) {
	  _rsListener = listener;
	}
	
	/**
	 * Invoked when a callback is received from ClusterManager for a storage resource coming ONLINE from OFFLINE
	 * 
	 * @param res Resource Key assigned for the storage node
	 * @return success Return value of adding the resource
	 * @throws Exception 
	 */
	public boolean addResource(String res) throws Exception
	{
		boolean result = false;
		logger.info("Attempting to add a resource " + res);
		if(_rsListener != null)
		{
		  _rsListener.addResource(res);
		  result = true;
		}
		return result;
	}

	/**
	 *  Invoked when a callback is received from ClusterManager for a storage resource going OFFLINE from ONLINE	
	 *
	 * @param res Resource key assigned for storage node
	 * @return success Return value of removing the resource
	 * @throws Exception 
	 */
	public boolean removeResource(String res) throws Exception
	{
		boolean result = false;
		logger.info("Attempting to remove a resource " + res);
		if(_rsListener != null)
		{
			_rsListener.removeResource(res);
			result = true;
		}
		return result;
	}
	
	public boolean dropDatabase(String dbName) throws Exception
	{
		boolean result = false;
		if(_rsListener != null)
		{
			_rsListener.dropDatabase(dbName);
			result = true;
		}
		return result;
	}

}
