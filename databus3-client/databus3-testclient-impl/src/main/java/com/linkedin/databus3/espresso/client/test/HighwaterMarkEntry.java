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


import java.util.HashMap;
import java.util.Map;

public abstract class HighwaterMarkEntry
{

  protected Map<String, String> _map;

  public HighwaterMarkEntry(Partition partition, EspressoSCN scn, String updater, String sessionID, long timestamp)
  {
    _map = new HashMap<String, String>();
    _map.put("Partition", partition.toString());
    _map.put("SCN", "" + scn.toString());
    _map.put("Updater", updater);
    _map.put("Session", sessionID);
    _map.put("Timestamp", "" + timestamp);
  }
  
  protected HighwaterMarkEntry(Map<String, String> map)
  {
    _map = map;
  }

  public String getUpdater()
  {
    return _map.get("Updater");

  }

  public Partition getPartition()
  {
    EspressoStateUnitKey key = new EspressoStateUnitKey(_map.get("Partition"));
    return new Partition(key.getDBName(), key.getPartitionId());
  }

  public String getSessionID()
  {
    return _map.get("Session");
  }

  public long getTimestamp()
  {
    return Long.parseLong(_map.get("Timestamp"));
  }

  public EspressoSCN getSCN()
  {
    return EspressoSCN.parseSCN(_map.get("SCN"));
  }
  
  public Map<String, String> getMap()
  {
    return _map;
  }
  
  public abstract String getMaster();
  
  @Override
  public String toString()
  {
    return _map.toString();
  }
}
