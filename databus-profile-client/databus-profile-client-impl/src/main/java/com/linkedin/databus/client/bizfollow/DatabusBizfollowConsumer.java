package com.linkedin.databus.client.bizfollow;
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
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.databus.client.generic.DatabusFileLoggingConsumer;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.events.bizfollow.bizfollow.BizFollow;

public class DatabusBizfollowConsumer extends DatabusFileLoggingConsumer
{
  public final static String MODULE = DatabusBizfollowConsumer.class.getName();
  public final static Logger LOG    = Logger.getLogger(MODULE);

  private BizFollow          _bizFollowReUse;
  private Map<Long,Long> _srcKeyScnMap = null;
  private long _sumScn = 0;
  private long _sumKey = 0;

  public DatabusBizfollowConsumer(String outputFilename) throws IOException
  {
    super(outputFilename, false);
    _srcKeyScnMap = new HashMap<Long,Long>();
  }

  public DatabusBizfollowConsumer() throws IOException
  {
    this(null);
  }

  private void checkEventPattern(DbusEvent e)
  {
    long key = e.key();
    long oldScn = 0;
    if (_srcKeyScnMap.containsKey(key))
    {
      oldScn = _srcKeyScnMap.get(key);
      LOG.info("==EventPattern key exists: scn = " + e.sequence() + ", key = " + key);
    }
    else
    {
      _sumKey += key;    // add the key
    }
    long scn = e.sequence();
    _sumScn += (scn - oldScn);
    _srcKeyScnMap.put(key, scn);
    LOG.info("==EventPattern: scn = " + scn + ", _sumScn = " + _sumScn + ", key = " + key + ", _sumKey = " + _sumKey);
  }

  @Override
  public ConsumerCallbackResult onEndDataEventSequence(SCN endScn) {
    if (isCheckingEventPattern())       // not checking what pattern yet
    {
      LOG.info("==EventPattern endDataEventSequence: scn = " + endScn + ", _sumScn = " + _sumScn + ", _sumKey = " + _sumKey);
      if (_sumScn != _sumKey)
      {
        LOG.error("==EventPattern Error in endDataEventSequence: scn = " + endScn + ", _sumScn = " + _sumScn + ", _sumKey = " + _sumKey);
//        pause();   // pause the consumer
      }
    }
    return super.onEndDataEventSequence(endScn);
  }

  @Override
  public ConsumerCallbackResult onEndBootstrapSequence(SCN endScn)
  {
    if (isCheckingEventPattern())        // not checking what pattern yet
    {
      LOG.info("==EventPattern endBootstrapSequence: scn = " + endScn + ", _sumScn = " + _sumScn + ", _sumKey = " + _sumKey);
      if (_sumScn != _sumKey)
      {
        LOG.error("==EventPattern Error in endBootstrapSequence: scn = " + endScn + ", _sumScn = " + _sumScn + ", _sumKey = " + _sumKey);
//        pause();   // pause the consumer
      }
    }
    return super.onEndBootstrapSequence(endScn);
  }

  @Override
  protected void LogTypedValue(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    if (null == _bizFollowReUse)
    {
      _bizFollowReUse = new BizFollow();
    }

    long timeDiff = System.nanoTime() - e.timestampInNanos();
    eventDecoder.getTypedValue(e, _bizFollowReUse, BizFollow.class);

    LOG.info("windowScn:" + e.sequence() + ", Id:" + _bizFollowReUse.id + ", age: (sec)"
        + (timeDiff / 1000000000.0));
    if (LOG.isDebugEnabled())
      LOG.debug(_bizFollowReUse);

    if (isCheckingEventPattern())
    {
      checkEventPattern(e);
    }
  }
}
