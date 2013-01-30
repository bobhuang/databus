package com.linkedin.databus.client.liar;
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

import org.apache.log4j.Logger;

import com.linkedin.databus.client.generic.DatabusFileLoggingConsumer;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.events.liar.jobrelay.LiarJobRelay;
import com.linkedin.events.liar.memberrelay.LiarMemberRelay;

public class DatabusLiarConsumer extends DatabusFileLoggingConsumer
{
  public final static String MODULE = DatabusLiarConsumer.class.getName();
  public final static Logger LOG    = Logger.getLogger(MODULE);

  private LiarJobRelay                        liarJobRelayReUse;
  private LiarMemberRelay                     liarMemberRelayReUse;
  public final static short LIAR_JOB_RELAY_SOURCE_ID = 20;
  public final static short LIAR_MEMBER_RELAY_SOURCE_ID = 21;

  public DatabusLiarConsumer(String outputFilename) throws IOException
  {
    this(outputFilename, false);
  }

  public DatabusLiarConsumer(String outputFilename, boolean append) throws IOException
  {
    super(outputFilename, append);
  }
  
  public DatabusLiarConsumer() throws IOException
  {
    this(null);
  }

  protected void LogTypedValue(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    short srcId = e.srcId();
    switch (srcId)
    {
      case LIAR_JOB_RELAY_SOURCE_ID:
        LogTypedValueLiarJobRelay(e, eventDecoder);
      case LIAR_MEMBER_RELAY_SOURCE_ID:
        LogTypedValueLiarMemberRelay(e, eventDecoder);
    }
  }

  protected void LogTypedValueLiarJobRelay(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    if (null == liarJobRelayReUse)
    {
      liarJobRelayReUse = new LiarJobRelay();
    }

    long timeDiff = System.nanoTime() - e.timestampInNanos();
    eventDecoder.getTypedValue(e, liarJobRelayReUse, LiarJobRelay.class);

    LOG.info("windowScn:" + e.sequence() + ", eventId:" + liarJobRelayReUse.eventId
        + ", age: (sec)" + (timeDiff / 1000000000.0));
    if (LOG.isDebugEnabled())
      LOG.debug(liarJobRelayReUse);
  }

  protected void LogTypedValueLiarMemberRelay(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    if (null == liarMemberRelayReUse)
    {
      liarMemberRelayReUse = new LiarMemberRelay();
    }

    long timeDiff = System.nanoTime() - e.timestampInNanos();
    eventDecoder.getTypedValue(e, liarMemberRelayReUse, LiarMemberRelay.class);

    LOG.info("windowScn:" + e.sequence() + ", eventId:" + liarMemberRelayReUse.eventId
        + ", age: (sec)" + (timeDiff / 1000000000.0));
    if (LOG.isDebugEnabled())
      LOG.debug(liarMemberRelayReUse);
  }
}
