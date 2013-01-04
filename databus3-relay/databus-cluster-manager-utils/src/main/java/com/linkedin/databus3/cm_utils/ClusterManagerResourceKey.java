package com.linkedin.databus3.cm_utils;

import java.text.ParseException;

/**
 * A class encapsulating the keys received from the clustermanager, as applicable to the relay use-case
 * Provides 
 * 
 * @author Phanindra Ganti<pganti@linkedin.com>
 */

public class ClusterManagerResourceKey
{
  private int NUM_SUBKEYS = 4;
  
  private String _physicalSource;
  private String _physicalPartition;
  private ClusterManagerLogicalPartitionRepresentation _logicalPartitionRepresentation;
  private boolean _isMaster = true;
  private boolean _isOnline = false;
  
  public ClusterManagerResourceKey(String resourceKey)
  throws ParseException
  {
    /**
     * For example: Resource "ela4-db1-espresso.prod.linkedin.com_1521,bizProfile,bizProfile,p1_1,MASTER" 
     * PS : "ela4-db1-espresso.prod.linkedin.com_1521"
         * PP : "bizProfile"
     * LP :  "p1_1"
     * state: "MASTER"
     */
    String[] parts = resourceKey.split(",");
    if (parts.length != NUM_SUBKEYS)
      throw new ParseException("Does not have " + NUM_SUBKEYS, parts.length);
    _physicalSource = parts[0];
    _physicalPartition = parts[1];
    _logicalPartitionRepresentation = new ClusterManagerLogicalPartitionRepresentation(parts[2]);
    _isMaster = parts[3].equalsIgnoreCase("MASTER");
    _isOnline = parts[3].equalsIgnoreCase("MASTER") || parts[3].equalsIgnoreCase("SLAVE");
  }

  public String getPhysicalSource() {
    return _physicalSource;
  }
  public void setPhysicalSource(String pSource) {
    _physicalSource = pSource;
  }
  public boolean isOnline() {
    return _isOnline;
  }

  public String getPhysicalPartition() {
    return _physicalPartition;
  }

  public String getLogicalPartition() {
    return _logicalPartitionRepresentation.getLogicalPartition();
  }
  public int getLogicalPartitionNumber() {
    return _logicalPartitionRepresentation.getPartitionNum();
  }
  public int getLogicalSchemaVersion() {
    return _logicalPartitionRepresentation.getSchemaVersion();
  }

  public boolean getIsMaster() {
    return _isMaster;
  }
  
  @Override
  public String toString() {
    return "pp=" + _physicalPartition + ";ps=" + _physicalSource + ";lp="+_logicalPartitionRepresentation + ";isMaster=" + _isMaster;
  }
}