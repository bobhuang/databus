package com.linkedin.databus3.espresso.client.test;
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


/**
 * Represents a partition unit.
 * A partition is specified by the DB Name and it's partition ID
 * @author aauradka
 *
 */
public class Partition implements Comparable<Partition>
{
  private final int _partitionId;
  private final String _dbName;

  public Partition(String dbName, int partitionId)
  {
    _partitionId = partitionId;
    _dbName = dbName;
  }

  public int getPartitionId()
  {
    return _partitionId;
  }

  public String getDbName()
  {
    return _dbName;
  }

  public static Partition parsePartition(String partitionStr)
  {
    int pos = partitionStr.lastIndexOf('_');
    if(pos == -1 || pos == partitionStr.length() - 1)
    {
      throw new IllegalArgumentException(partitionStr);
    }

    return new Partition(partitionStr.substring(0,pos), Integer.valueOf(partitionStr.substring(pos + 1)));
  }

  @Override
  public int hashCode()
  {
    final int prime = 99991;
    int result = prime * _partitionId + ((_dbName == null) ? 0 : _dbName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (this == obj)
    {
      return true;
    }
    if (obj == null)
    {
      return false;
    }
    if (! (obj instanceof Partition))
    {
      return false;
    }

    Partition p = (Partition) obj;
    if(p.getDbName().equals(_dbName) && p.getPartitionId() == _partitionId)
    {
      return true;
    }

    return false;
  }

  @Override
  public String toString()
  {
    // Don't change this as this without fixing unit tests that depend on partitions
    // serializing this way:
    return _dbName + "_" + _partitionId;
  }

  @Override
  public int compareTo(Partition that)
  {
    if (this == that || this.equals(that))
    {
      return 0;
    }

    if(this._dbName.equals(that._dbName))
      return (this._partitionId - that._partitionId);

    return this._dbName.compareTo(that._dbName);
  }
}
