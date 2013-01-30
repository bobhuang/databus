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



import java.io.IOException;
import java.io.OutputStream;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.locks.Lock;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.log4j.Logger;

import com.linkedin.databus.core.monitoring.mbean.AbstractMonitoringMBean;
import com.linkedin.databus.core.monitoring.mbean.DatabusMonitoringMBean;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.StatsCollectorMergeable;
import com.linkedin.databus3.rpl_dbus_manager.RplDbusState;

/**
 * collect stats for each instance of RPLDbus thread
 *
 */
public class RplDbusTotalStats extends AbstractMonitoringMBean<RplDbusTotalStatsEvent>
implements RplDbusTotalStatsMBean,
StatsCollectorMergeable<RplDbusTotalStats>
{
  public static final String MODULE = RplDbusTotalStats.class.getName();

  private final String _dimension;

  public RplDbusTotalStats(int ownerId, String dimension,
                           boolean enabled, boolean threadSafe,
                           RplDbusTotalStatsEvent initData)
  {
    super(enabled, threadSafe, initData);
    _dimension = AbstractMonitoringMBean.sanitizeString(dimension);
    _event.ownerId = ownerId;
    _event.dimension = _dimension;
    _event.timestampCreated= System.currentTimeMillis();
    _event.timestampRecentStart= 0;
    _event.timestampRecentMysqlConnect= 0;
    reset();
  }

  public RplDbusTotalStats clone(boolean threadSafe)
  {
    return new RplDbusTotalStats(_event.ownerId, _dimension, _enabled.get(), threadSafe,
                                 getStatistics(null));
  }

  public String getSanitizedName() {
    return _dimension;
  }

  // the actual metrics
  @Override
  public long getTimestampLastResetMs()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try {
      result = _event.timestampLastResetMs;
    } finally {
      releaseLock(readLock);
    }
    return result;
  }

  @Override
  public long getTimeSinceLastResetMs()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try {
      result = _event.timeSinceLastResetMs;
    } finally {
      releaseLock(readLock);
    }
    return result;

  }

  /**
   * time since the rpldbus thread was created
   */
  @Override
  public long getTimeSinceCreation()
  {
    Lock readLock = acquireReadLock();
    try {
      return System.currentTimeMillis()- _event.timestampCreated;
    }
    finally {
      releaseLock(readLock);
    }
  }

  public void registerCreationTime(long s)
  {
    Lock writeLock = acquireWriteLock();

    try {
      _event.timestampCreated = s;
    } finally {
      releaseLock(writeLock);
    }
  }

  /**
   * time since the rpldbus thread was started
   */
  @Override
  public long getTimeSinceStart()
  {
    Lock readLock = acquireReadLock();
    try {
      return System.currentTimeMillis()- _event.timestampRecentStart;
    }
    finally {
      releaseLock(readLock);
    }
  }

  public void registerStartTime(long s)
  {
    Lock writeLock = acquireWriteLock();

    try {
      _event.timestampRecentStart = s;
    } finally {
      releaseLock(writeLock);
    }
  }

  @Override
  public int getIOErrors()
  {
    Lock readLock = acquireReadLock();
    try {
      return _event.ioErrors;
    }
    finally {
      releaseLock(readLock);
    }
  }

  @Override
  public int getSQLErrors()
  {
    Lock readLock = acquireReadLock();
    try {
      return _event.sqlErrors;
    }
    finally {
      releaseLock(readLock);
    }
  }

  /**
   * time since we connected to RPLDBUS
   */
  @Override
  public long getTimeSinceConnect()
  {
    Lock readLock = acquireReadLock();
    try {
      return System.currentTimeMillis()- _event.timestampRecentMysqlConnect;
    }
    finally {
      releaseLock(readLock);
    }
  }

  public void registerConnectTime(long s)
  {
    Lock writeLock = acquireWriteLock();

    try {
      _event.timestampRecentMysqlConnect = s;
    } finally {
      releaseLock(writeLock);
    }
  }


  /**
   * how far IO thread in rpldbus thread is behind its master
   */
  @Override
  public long getRplDbusBehindMaster() {
    Lock readLock = acquireReadLock();
    long result = 0;
    try {
      result = _event.rplDbusBehindMaster;
    } finally {
      releaseLock(readLock);
    }
    return result;
  }

  /**
   * number of times we had to call change master on this rpldbus thread
   */
  @Override
  public int getNumChangeMasterCalled() {
    Lock readLock = acquireReadLock();
    int result = 0;
    try {
      result = _event.numChangeMasterCalled;
    } finally {
      releaseLock(readLock);
    }
    return result;
  }

  /**
   * number of time we ran into STOPPED thread situation
   */
  @Override
  public int getStoppedThreads() {
    Lock readLock = acquireReadLock();
    int result = 0;
    try {
      result = _event.stoppedThreads;
    } finally {
      releaseLock(readLock);
    }
    return result;
  }

  // register calls for the metrics
  public void registerChangeMasterCalled()
  {

    if (! _enabled.get()) return;
    Lock writeLock = acquireWriteLock();

    try {
      _event.numChangeMasterCalled ++;
    } finally {
      releaseLock(writeLock);
    }
  }

  public void registerState(RplDbusState state)
  {
    if(state == null)
      return;

    if (! _enabled.get()) return;
    Lock writeLock = acquireWriteLock();

    try {
      _event.rplDbusBehindMaster = state.getSecondsBehindMaster();
      _event.stoppedThreads = state.isRplDbusUp()? 0 : 1;
      _event.ioErrors = state.getIOError().isEmpty() ? 0 : 1; // count errors
      _event.sqlErrors = state.getSQLError().isEmpty() ? 0 : 1;
    } finally {
      releaseLock(writeLock);
    }
  }

  @Override
  protected void resetData()
  {
    _event.timestampLastResetMs = System.currentTimeMillis();
    _event.timeSinceLastResetMs = 0;
    _event.timestampCreated = System.currentTimeMillis();
    _event.timestampRecentStart = System.currentTimeMillis();
    _event.timestampRecentMysqlConnect = System.currentTimeMillis();
    _event.rplDbusBehindMaster = 0;
    _event.numChangeMasterCalled = 0;
    _event.stoppedThreads = 0;
    _event.ioErrors = 0;
    _event.sqlErrors = 0;
  }

  @Override
  public JsonEncoder createJsonEncoder(OutputStream out) throws IOException
  {
    throw new RuntimeException("JSON encoding is not supported");
  }

  @Override
  protected void cloneData(RplDbusTotalStatsEvent event)
  {
    event.ownerId = _event.ownerId;
    event.dimension = _event.dimension;
    event.timestampLastResetMs = _event.timestampLastResetMs;
    event.timeSinceLastResetMs = System.currentTimeMillis() - _event.timestampLastResetMs;
    event.timestampCreated = _event.timestampCreated;
    event.timestampRecentStart = _event.timestampRecentStart;
    event.timestampRecentMysqlConnect = _event.timestampRecentMysqlConnect;
    event.rplDbusBehindMaster = _event.rplDbusBehindMaster;
    event.numChangeMasterCalled = _event.numChangeMasterCalled;
    event.stoppedThreads = _event.stoppedThreads;
    event.ioErrors = _event.ioErrors;
    event.sqlErrors = _event.sqlErrors;
  }


  @Override
  public void resetAndMerge(List<RplDbusTotalStats> objList) 
  {
	  Lock writeLock = acquireWriteLock();
	  try
	  {
		  reset();
		  for (RplDbusTotalStats t: objList)
		  {
			  merge(t);
		  }
	  }
	  finally
	  {
		  releaseLock(writeLock);
	  }
  }
  
  @Override
  protected RplDbusTotalStatsEvent newDataEvent()
  {
    return new RplDbusTotalStatsEvent();
  }

  @Override
  protected SpecificDatumWriter<RplDbusTotalStatsEvent> getAvroWriter()
  {
    //return new SpecificDatumWriter<RplDbusTotalStatsEvent>(RplDbusTotalStatsEvent.class);
    return null;
  }

  @Override
  public void mergeStats(DatabusMonitoringMBean<RplDbusTotalStatsEvent> other)
  {
    throw new RuntimeException("merge is not supported");
  }

  @Override
  protected void doMergeStats(Object eventData)
  {
    throw new RuntimeException("merge is not supported");
  }

  @Override
  public ObjectName generateObjectName() throws MalformedObjectNameException
  {
    Hashtable<String, String> mbeanProps = generateBaseMBeanProps();
    mbeanProps.put("ownerId", Integer.toString(_event.ownerId));
    mbeanProps.put("dimension", _dimension);

    return new ObjectName(AbstractMonitoringMBean.JMX_DOMAIN, mbeanProps);
  }

  public String getDimension()
  {
    return _dimension;
  }

  @Override
  public void merge(RplDbusTotalStats obj)
  {
    throw new RuntimeException("merge is not supported");
  }

  @Override
  public String toString() {
    return _dimension;
  }

}
