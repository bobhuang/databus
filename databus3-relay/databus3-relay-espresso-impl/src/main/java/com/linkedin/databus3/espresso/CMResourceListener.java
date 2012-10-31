package com.linkedin.databus3.espresso;

import java.text.ParseException;

import com.linkedin.databus2.core.DatabusException;

/**
 * knows how to react to add/remove resource notifications (for example from Cluster manager)
 * 
 */
public interface CMResourceListener
{

  /**
   * Interface methods to add / remove a partition	
   * @param rs ResourceKey
   * @throws DatabusException
   * @throws ParseException
   */
  public void addResource(String rs) throws DatabusException, ParseException;
  public void removeResource(String rs)  throws DatabusException, ParseException;

  /**
   * Interface to drop a database
   */
  public void dropDatabase(String dbName) throws DatabusException;
}
