package com.linkedin.databus.client.conns;
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
import com.linkedin.events.conns.Connections_V2;

public class DatabusConnectionsConsumer extends DatabusFileLoggingConsumer
{
  public final static String MODULE = DatabusConnectionsConsumer.class.getName();
  public final static Logger LOG    = Logger.getLogger(MODULE);

  private Connections_V2                        connsReUse;

  public DatabusConnectionsConsumer(String outputFilename) throws IOException
  {
    super(outputFilename, false);
  }

  public DatabusConnectionsConsumer() throws IOException
  {
    this(null);
  }

  @Override
  protected void LogTypedValue(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    if (null == connsReUse)
    {
      connsReUse = new Connections_V2();
    }

    long timeDiff = System.nanoTime() - e.timestampInNanos();
    eventDecoder.getTypedValue(e, connsReUse, Connections_V2.class);

    LOG.info("windowScn:" + e.sequence() + ", sourceId:" + connsReUse.sourceId
        + ", destId:" + connsReUse.destId
        + ", age: (sec)" + (timeDiff / 1000000000.0));
    if (LOG.isDebugEnabled())
      LOG.debug(connsReUse);
  }

}
