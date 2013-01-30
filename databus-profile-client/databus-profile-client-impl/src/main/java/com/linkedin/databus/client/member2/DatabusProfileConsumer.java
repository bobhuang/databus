package com.linkedin.databus.client.member2;
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
import com.linkedin.events.member2.profile.MemberProfile_V3;

public class DatabusProfileConsumer extends DatabusFileLoggingConsumer
{
  public final static String MODULE = DatabusProfileConsumer.class.getName();
  public final static Logger LOG    = Logger.getLogger(MODULE);

  private MemberProfile_V3                        profileReUse;

  public DatabusProfileConsumer(String outputFilename) throws IOException
  {
    super(outputFilename, false);
  }

  public DatabusProfileConsumer() throws IOException
  {
    this(null);
  }

  @Override
  protected void LogTypedValue(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    if (null == profileReUse)
    {
      profileReUse = new MemberProfile_V3();
    }

    long timeDiff = System.nanoTime() - e.timestampInNanos();
    eventDecoder.getTypedValue(e, profileReUse, MemberProfile_V3.class);

    LOG.info("windowScn:" + e.sequence() + ", memberId:" + profileReUse.memberId
        + ", age: (sec)" + (timeDiff / 1000000000.0));
    if (LOG.isDebugEnabled())
      LOG.debug(profileReUse);
  }

}
