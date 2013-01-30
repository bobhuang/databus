package com.linkedin.databus3.rpl_dbus_manager.monitoring.mbean;
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
  public int getStoppedThreads();

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
