package com.linkedin.databus3.rpl_dbus_manager;

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