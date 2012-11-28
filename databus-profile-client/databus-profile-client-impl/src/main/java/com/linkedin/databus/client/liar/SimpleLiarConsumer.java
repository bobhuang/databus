package com.linkedin.databus.client.liar;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.databus.client.generic.DatabusFileLoggingConsumer;
import com.linkedin.databus.client.generic.SimpleFileLoggingConsumer;

public class SimpleLiarConsumer extends SimpleFileLoggingConsumer {

  public static final String MODULE = SimpleLiarConsumer.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public static final String LIAR_JOB_RELAY_SOURCE_NAME = "com.linkedin.events.liar.jobrelay.LiarJobRelay";
  public static final String LIAR_MEMBER_RELAY_SOURCE_NAME = "com.linkedin.events.liar.memberrelay.LiarMemberRelay";

  protected List<String> addSources()
  {
    List<String> sources = new ArrayList<String>();
    sources.add(LIAR_JOB_RELAY_SOURCE_NAME);
    sources.add(LIAR_MEMBER_RELAY_SOURCE_NAME);
    return sources;
  }

  protected DatabusFileLoggingConsumer createTypedConsumer(String valueDumpFile) throws IOException
  {
    return new DatabusLiarConsumer(valueDumpFile);
  }
  public static void main(String args[]) throws Exception
  {
    SimpleLiarConsumer simpleLiarConsumer = new SimpleLiarConsumer();
    simpleLiarConsumer.mainFunction(args);
  }

}
