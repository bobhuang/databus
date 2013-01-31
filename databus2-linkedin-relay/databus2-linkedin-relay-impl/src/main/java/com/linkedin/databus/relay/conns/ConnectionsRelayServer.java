package com.linkedin.databus.relay.conns;
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
import com.linkedin.databus2.core.container.request.ContainerOperationProcessor;
import com.linkedin.databus2.core.container.request.RequestProcessorRegistry;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;

public class ConnectionsRelayServer extends HttpRelay
{
  public static final String MODULE = ConnectionsRelayServer.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public ConnectionsRelayServer() throws IOException, InvalidConfigException,
                                       DatabusException
  {
    this(new HttpRelay.Config(), null);
  }

  public ConnectionsRelayServer(HttpRelay.Config config, PhysicalSourceStaticConfig [] pConfigs)
  throws IOException, InvalidConfigException,  DatabusException
  {
    this(config.build(), pConfigs);
  }

  public ConnectionsRelayServer(HttpRelay.StaticConfig config, PhysicalSourceStaticConfig [] pConfigs)
  throws IOException, InvalidConfigException, DatabusException
  {
    super(config, pConfigs);
    initializeConnectionsCommandProcessors();
  }

  protected void initializeConnectionsCommandProcessors() throws DatabusException
  {
    RequestProcessorRegistry processorRegistry = getProcessorRegistry();
    DatabusEventProducer bizfollowEventProducer =
        new DatabusEventConnectionsRandomProducer(getEventBuffer(), 10, 100,
                                                getRelayStaticConfig().getSourceIds(),
                                                getSchemaRegistryService(),
                                                getRelayStaticConfig().getRandomProducer());

    processorRegistry.register(
        GenerateDataEventsRequestProcessor.COMMAND_NAME,
        new GenerateDataEventsRequestProcessor(null,
                                               this,
                                               bizfollowEventProducer));
   processorRegistry.register(
       LoadDataEventsRequestProcessor.COMMAND_NAME,
       new LoadDataEventsRequestProcessor(getDefaultExecutorService(), this));

   processorRegistry.register(ContainerOperationProcessor.COMMAND_NAME,
                              new ContainerOperationProcessor(null, this));
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    Properties startupProps = HttpRelay.processCommandLineArgs(args);
    Config config = new Config();

    config.getContainer().setIdFromName("BizFollowRelayServer.localhost");

    ConfigLoader<StaticConfig> staticConfigLoader =
        new ConfigLoader<StaticConfig>("databus.relay.", config);

    config.setSourceName(String.valueOf(DatabusEventConnectionsRandomProducer.CONNS_SRC_ID),
                         DatabusEventConnectionsRandomProducer.FULLY_QUALIFIED_CONNECTIONS_EVENT_NAME);

    HttpRelay.StaticConfig staticConfig = staticConfigLoader.loadConfig(startupProps);
    LOG.info("source = " + staticConfig.getSourceIds());

    ConnectionsRelayServer serverContainer = new ConnectionsRelayServer(staticConfig, null);
    serverContainer.startAndBlock();
  }
}
