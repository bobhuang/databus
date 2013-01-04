package com.linkedin.databus3.rpl_dbus_manager.monitoring.mbean;

/**
 * RplDbus statistics
 *
 */
public class RplDbusTotalStatsEvent
{
  /** The id of the owner (rpldbus manager) that generated the event */
  public int ownerId;

  /** dimension */
  public java.lang.CharSequence dimension;

  /** unix timestamp of creation of the rpldbus thread */
  public long timestampCreated;

  /** unix timestamp of last start */
  public long timestampRecentStart;

  /** unix timestamp of last connect to mysql of RPLDBUS */
  public long timestampRecentMysqlConnect;

  /** unix timestamp of the last reset() call */
  public long timestampLastResetMs;

  /** time in ms since the last reset() call */
  public long timeSinceLastResetMs;

  /** Number of times RPLDBUS thread was found stopped total*/
  public int stoppedThreads;

  /** number change master was called */
  public int numChangeMasterCalled;

  /** seconds rpldbus is behind the master */
  public long rplDbusBehindMaster;

  /** total count of IO thread errors */
  public int ioErrors;

  /** total count of SQL thread errors */
  public int sqlErrors;
}