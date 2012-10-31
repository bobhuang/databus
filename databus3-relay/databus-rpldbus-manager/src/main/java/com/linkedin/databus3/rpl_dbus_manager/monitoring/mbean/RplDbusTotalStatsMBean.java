package com.linkedin.databus3.rpl_dbus_manager.monitoring.mbean;

import com.linkedin.databus.core.monitoring.mbean.DatabusMonitoringMBean;


public interface RplDbusTotalStatsMBean  extends DatabusMonitoringMBean<RplDbusTotalStatsEvent>
{
  // ************** GETTERS *********************

  /** time since the rpldbus thread was created */
  public long getTimeSinceCreation();

  /** time since the rpldbus thread was started */
  long getTimeSinceStart();

  /** time since we connected to RPLDBUS most recently */
  long getTimeSinceConnect();

  /** Number of times RPLDBUS thread was stopped */
  public int getNumStoppedThreads();

  /** unix timestamp of the last reset() call */
  @Override
  public long getTimestampLastResetMs();

  /** time in ms since the last reset() call */
  @Override
  public long getTimeSinceLastResetMs();

  /** number change master was called */
  public int getNumChangeMasterCalled();

  /** seconds rpldbus is behind the master */
  public long getRplDbusBehindMaster();

  /** number of io thread errors */
  public int getIOErrors();

  /** number of sql thread errors */
  public int getSQLErrors();

    /** Resets the statistics. */
  @Override
  void reset();



}
