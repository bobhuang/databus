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


import org.I0Itec.zkclient.DataUpdater;

import com.linkedin.helix.AccessOption;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.store.HelixPropertyStore;

public class PropertyStoreBasedHighwaterMarkStore implements HighwaterMarkBackingStore<ZNRecord>
{
  private HelixPropertyStore<ZNRecord> _propertyStore;

  public PropertyStoreBasedHighwaterMarkStore(HelixPropertyStore<ZNRecord> propertyStore)
  {
    _propertyStore = propertyStore;
  }
  
  @Override
  public void put(String path, DataUpdater<ZNRecord> updater) throws Exception
  {
    _propertyStore.update(path, updater, AccessOption.PERSISTENT);
  }

  @Override
  public ZNRecord get(String path) throws Exception
  {
    try
    {
      return _propertyStore.get(path, null, AccessOption.PERSISTENT);
    }
    catch(RuntimeException e)
    {
      return null;
    }
  }

  @Override
  public void put(String path, ZNRecord updatedProperty) throws Exception
  {
    _propertyStore.set(path, updatedProperty, AccessOption.PERSISTENT);
  }
}
