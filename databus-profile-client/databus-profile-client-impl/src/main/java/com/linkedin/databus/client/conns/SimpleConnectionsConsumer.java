package com.linkedin.databus.client.conns;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.databus.client.generic.DatabusFileLoggingConsumer;
import com.linkedin.databus.client.generic.SimpleFileLoggingConsumer;

public class SimpleConnectionsConsumer extends SimpleFileLoggingConsumer {

  public static final String MODULE = SimpleConnectionsConsumer.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public static final String CONNECTIONS_SOURCE_NAME = "com.linkedin.events.conns.Connections";

  @Override
  protected String[] addSources()
  {
	String[] sources =  new String[] {CONNECTIONS_SOURCE_NAME};
    return sources;
  }

  @Override
  protected DatabusFileLoggingConsumer createTypedConsumer(String valueDumpFile) throws IOException
  {
    return new DatabusConnectionsConsumer(valueDumpFile);
  }
  public static void main(String args[]) throws Exception
  {
    SimpleConnectionsConsumer simpleProfileConsumer = new SimpleConnectionsConsumer();
    simpleProfileConsumer.mainFunction(args);
  }

}
