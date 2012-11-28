package com.linkedin.databus.client.bizfollow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.databus.client.generic.DatabusFileLoggingConsumer;
import com.linkedin.databus.client.generic.SimpleFileLoggingConsumer;

public class SimpleBizfollowConsumer extends SimpleFileLoggingConsumer {

  public static final String MODULE = SimpleBizfollowConsumer.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public static final String BIZFOLLOW_SOURCE_NAME = "com.linkedin.events.bizfollow.bizfollow.BizFollow";

  protected List<String> addSources()
  {
    List<String> sources = new ArrayList<String>();
    sources.add(BIZFOLLOW_SOURCE_NAME);
    return sources;
  }

  protected DatabusFileLoggingConsumer createTypedConsumer(String valueDumpFile) throws IOException
  {
    return new DatabusBizfollowConsumer(valueDumpFile);
  }

  public static void main(String args[]) throws Exception
  {
    SimpleBizfollowConsumer simpleBizFollowConsumer = new SimpleBizfollowConsumer();
    simpleBizFollowConsumer.mainFunction(args);
  }
}
