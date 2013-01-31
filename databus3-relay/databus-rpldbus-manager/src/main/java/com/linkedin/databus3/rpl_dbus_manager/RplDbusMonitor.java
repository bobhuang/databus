package com.linkedin.databus3.rpl_dbus_manager;
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


import org.apache.log4j.Logger;

public class RplDbusMonitor implements RplDbusMonitorMBean
{
  
  public static final String MODULE = RplDbusMonitor.class.getName();
  public static final Logger LOG    = Logger.getLogger(MODULE);
  public static final int UPDATE_THRESHOLD_SEC = 1;
  
  
  private final RplDbusAdapter _rplDbusAdapter;
  private RplDbusState _currRplDbusState = null;
  private final RplDbusMysqlCoordinates _snMySql;
  

  public RplDbusMonitor(RplDbusAdapter adapter, RplDbusMysqlCoordinates snMyql)
  {
    _rplDbusAdapter = adapter;
    _snMySql = snMyql;
  }
  
  private synchronized void  updateStatus() throws RplDbusException
  {
    if(_currRplDbusState == null || _currRplDbusState.secFromUpdate()>UPDATE_THRESHOLD_SEC) {
      _currRplDbusState = _rplDbusAdapter.getState(_snMySql);
    }
  }
  

  public boolean getSlaveSQLThreadRunning() throws RplDbusException
  {
    updateStatus();
    return _currRplDbusState.getSlaveSQLThreadRunning();
  }
  
  public boolean getSlaveIOThreadRunning() throws RplDbusException
  {
    updateStatus();
    return _currRplDbusState.getSlaveIOThreadRunning();
  }
  
  public String getMasterHost() throws RplDbusException
  {
    updateStatus();
    return _currRplDbusState.getMasterHost();
  }
  
  public int getMasterPort() throws RplDbusException
  {
    updateStatus();
    return _currRplDbusState.getMasterPort();
  }
  
  public int getMasterServerID() throws RplDbusException
  {
    updateStatus();
    return _currRplDbusState.getMasterServerid();
  }
  
  public long getSecondsBehindMaster() throws RplDbusException
  {
    updateStatus();
    return _currRplDbusState.getSecondsBehindMaster();
  }
  
  public String toStringWithException() throws RplDbusException
  {
    updateStatus();
    return _currRplDbusState.toString();
  }
  
  @Override
  public String toString() {
    try {
      return toStringWithException();
    } catch (RplDbusException e) {
      return "Failed to get data: " + e.getLocalizedMessage();
    }
  }
}
