package com.linkedin.databus3.rpl_dbus_manager;

public interface RplDbusMonitorMBean
{
  public boolean getSlaveSQLThreadRunning() throws RplDbusException;
  public boolean getSlaveIOThreadRunning() throws RplDbusException;
  public String getMasterHost() throws RplDbusException;
  public int getMasterPort() throws RplDbusException;
  public int getMasterServerID() throws RplDbusException;
  public long getSecondsBehindMaster() throws RplDbusException;
  
}