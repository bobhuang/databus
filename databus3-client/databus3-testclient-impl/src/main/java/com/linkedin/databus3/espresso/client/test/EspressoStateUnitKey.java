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


public class EspressoStateUnitKey
{
  private String _dbName;
  private int _partitionId;

  EspressoStateUnitKey(String stateUnitKey)
  {
    int partitionNumberPos = stateUnitKey.lastIndexOf("_");
    assert(partitionNumberPos > 0);
    
    _dbName = stateUnitKey.substring(0, partitionNumberPos);
    _partitionId = Integer.parseInt(stateUnitKey.substring(partitionNumberPos + 1, stateUnitKey.length()));
  }
  
  public String getDBName()
  {
    return _dbName;
  }
  
  public int getPartitionId()
  {
    return _partitionId;
  }
  
  public String toString()
  {
    return _dbName + "_" + _partitionId;
  }
}
