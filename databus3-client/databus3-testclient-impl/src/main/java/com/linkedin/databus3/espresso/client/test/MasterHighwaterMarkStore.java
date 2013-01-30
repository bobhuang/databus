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


import java.util.Iterator;
import com.linkedin.helix.ZNRecord;

//all the getters can return null if the entries don't exist 
public interface MasterHighwaterMarkStore
{
  public EspressoSCN getLatestSCN(Partition partition) throws Exception;

  public EspressoSCN getSCNForGenerationStart(Partition partition, int generation) throws Exception;

  public EspressoSCN getSCNForGenerationEnd(Partition partition, int generation) throws Exception;
  
  //convenience method - return getSCNForGenerationEnd() if non-null. otherwise, return getSCNForGenerationStart()
  public EspressoSCN getSCNForGeneration(Partition partition, int generation) throws Exception;

  public String getLatestMaster(Partition partition) throws Exception;
  
  public String getMasterForGeneration(Partition partition, int generation) throws Exception;
  
  public Iterator<MasterHighwaterMarkEntry> getEntriesIterator(Partition partition) throws Exception;
  
  public ZNRecord getProperty(Partition partition) throws Exception;
  
  public MasterHighwaterMarkEntry getLatestEntry(Partition partition) throws Exception;
  
  public MasterHighwaterMarkEntry prepareNewEntry(Partition partition,
                                                  EspressoSCN prevGenEndSCN,
                                                  EspressoSCN newGenStartSCN,
                                                  String prevMaster,
                                                  String updater,
                                                  String sessionID);

  public void write(MasterHighwaterMarkEntry entry) throws Exception;
  
  public void write(ZNRecord currentRecord, MasterHighwaterMarkEntry entry) throws Exception;

}
