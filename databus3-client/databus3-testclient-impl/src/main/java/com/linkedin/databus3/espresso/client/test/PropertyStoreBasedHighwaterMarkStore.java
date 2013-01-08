package com.linkedin.databus3.espresso.client.test;

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
