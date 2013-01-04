package com.linkedin.databus.client.member2;

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
