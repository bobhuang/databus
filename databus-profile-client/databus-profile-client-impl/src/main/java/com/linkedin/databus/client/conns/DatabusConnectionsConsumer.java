package com.linkedin.databus.client.conns;

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
