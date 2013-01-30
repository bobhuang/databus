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


import java.util.Map;


/**
 * Represent a master high water mark entry
 * @author sjagadis
 *
 */
public class MasterHighwaterMarkEntry extends HighwaterMarkEntry
{

  public MasterHighwaterMarkEntry(Partition partition,
                                  EspressoSCN prevGenEndSCN,
                                  EspressoSCN newGenStartSCN,
                                  String prevMaster,
                                  String updater,
                                  String sessionID,
                                  long timestamp)
  {
    super(partition, newGenStartSCN, updater, sessionID, timestamp);
    _map.put("Master", updater); //XXX: Remove this after fixing dependencies
    if(prevMaster != null)
    {
      _map.put("PrevMaster", prevMaster);
    }
    
    if(prevGenEndSCN != null)
    {
      _map.put("PrevGenEndSCN", prevGenEndSCN.toString());
    }
  }
  
  MasterHighwaterMarkEntry(Map<String, String> map)
  {
    super(map);
  }

  /**
   * 
   * @return master for the previous generation if any. null if there is no previous generation
   */
  public String getPrevMaster()
  {
    return _map.get("PrevMaster");
  }

  /**
   * 
   * @return SCN corresponding to end of previous generation. null if no prev gen
   */
  public EspressoSCN getPrevGenerationEnd()
  {
    return (_map.containsKey("PrevGenEndSCN") ? EspressoSCN.parseSCN(_map.get("PrevGenEndSCN")) : null);
  }

  /**
   * @return the master for the current generation
   */
  @Override
  public String getMaster()
  {
    return getUpdater();
  }
}
