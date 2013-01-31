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

  protected String[] addSources()
  {
	String[] sources = new String[] {LIAR_JOB_RELAY_SOURCE_NAME, LIAR_MEMBER_RELAY_SOURCE_NAME};
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
