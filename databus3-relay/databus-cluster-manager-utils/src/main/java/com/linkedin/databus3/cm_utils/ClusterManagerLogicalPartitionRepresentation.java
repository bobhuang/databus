package com.linkedin.databus3.cm_utils;

import java.text.ParseException;

public class ClusterManagerLogicalPartitionRepresentation
{
  private int _partitionNum;
  private int _schemaVersion;
  private String _logicalPartition;
  
  ClusterManagerLogicalPartitionRepresentation(String logicalPartition)
  throws ParseException, NumberFormatException
  {
    /**
     * Validate that logicalPartition is represented in the format p10_1 ( partition 10, schema 1)
     */
    if (logicalPartition.charAt(0) != 'p')
    {
      throw new ParseException(logicalPartition, 0);
    }
    String nextStr = logicalPartition.substring(1);
    String[] parts = nextStr.split("_");
    
    if (parts.length != 2)
    {
      throw new ParseException("logical Partition does not have numbers of format p[num1]_[num2]" + logicalPartition, 0);
    }
    int p = Integer.parseInt(parts[0]);
    int s = Integer.parseInt(parts[1]); 
    
    _logicalPartition = logicalPartition;
    _partitionNum = p;
    _schemaVersion = s;     
  }
  
  ClusterManagerLogicalPartitionRepresentation(int partitionNum, int schemaVersion)
  {
    _partitionNum = partitionNum;
    _schemaVersion = schemaVersion;
    _logicalPartition = "p" + Integer.toString(partitionNum) + "_" + Integer.toString(schemaVersion);
  }
  
  public int getPartitionNum()
  {
    return _partitionNum;
  }
  
  public int getSchemaVersion()
  {
    return _schemaVersion;
  }

  public String getLogicalPartition()
  {
    return _logicalPartition;
  }

  @Override
  public String toString() {
    return "pn=" + _partitionNum + ";sv=" + _schemaVersion + ";" + _logicalPartition;
  }
}
