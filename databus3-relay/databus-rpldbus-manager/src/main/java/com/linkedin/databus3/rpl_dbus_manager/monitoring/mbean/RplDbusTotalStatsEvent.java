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
