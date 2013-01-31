package com.linkedin.databus.relay.member2;
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
import java.util.Properties;

import org.apache.log4j.Logger;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.container.request.GenerateDataEventsRequestProcessor;
import com.linkedin.databus.container.request.LoadDataEventsRequestProcessor;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.DatabusEventProducer;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.request.RequestProcessorRegistry;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;

public class Member2RelayServer extends HttpRelay {
  public static final String MODULE = Member2RelayServer.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  static final String FULLY_QUALIFIED_PROFILE_EVENT_NAME = "com.linkedin.events.member2.profile.MemberProfile";
  static final int MEMBER_PROFILE_SRC_ID = 40;

  public Member2RelayServer() throws IOException, InvalidConfigException, DatabusException
  {
    this(new HttpRelay.Config(), null);
  }

  public Member2RelayServer(HttpRelay.Config config, PhysicalSourceStaticConfig [] pConfigs)
  throws IOException, InvalidConfigException, DatabusException
  {
    this(config.build(), pConfigs);
  }

  public Member2RelayServer(HttpRelay.StaticConfig config, PhysicalSourceStaticConfig [] pConfigs)
  throws IOException, InvalidConfigException, DatabusException
  {
    super(config, pConfigs);
    initializeMember2RelayCommandProcessors();
  }

  protected void initializeMember2RelayCommandProcessors() throws DatabusException
  {
    RequestProcessorRegistry processorRegistry = getProcessorRegistry();
    DatabusEventProducer profileEventProducer =
        new DatabusEventProfileRandomProducer(getEventBuffer(), 10, 100,
                                              getRelayStaticConfig().getSourceIds(),
                                              getSchemaRegistryService(),
                                              getRelayStaticConfig().getRandomProducer());

    processorRegistry.register(
        GenerateDataEventsRequestProcessor.COMMAND_NAME,
        new GenerateDataEventsRequestProcessor(null,
                                               this,
                                               profileEventProducer));
   processorRegistry.register(
       LoadDataEventsRequestProcessor.COMMAND_NAME,
       new LoadDataEventsRequestProcessor(getDefaultExecutorService(), this));
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    Properties startupProps = HttpRelay.processCommandLineArgs(args);
    Config config = new Config();

    config.getContainer().setIdFromName("Member2RelayServer.localhost");

    ConfigLoader<StaticConfig> staticConfigLoader =
        new ConfigLoader<StaticConfig>("databus.relay.", config);

    config.setSourceName(String.valueOf(MEMBER_PROFILE_SRC_ID), FULLY_QUALIFIED_PROFILE_EVENT_NAME);

    HttpRelay.StaticConfig staticConfig = staticConfigLoader.loadConfig(startupProps);
    LOG.info("source = " + staticConfig.getSourceIds());

    Member2RelayServer serverContainer = new Member2RelayServer(staticConfig, null);

    serverContainer.startAndBlock();
  }
}
