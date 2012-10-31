package com.linkedin.databus.relay.bizfollow;

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

public class BizFollowRelayServer extends HttpRelay
{
  public static final String MODULE = BizFollowRelayServer.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  private static final String FULLY_QUALIFIED_BIZFOLLOW_EVENT_NAME = "com.linkedin.events.bizfollow.bizfollow.BizFollow";
  private static final int BIZFOLLOW_SRC_ID = 40;

  public BizFollowRelayServer() throws IOException, InvalidConfigException,
                                       DatabusException
  {
    this(new HttpRelay.Config(), null);
  }

  public BizFollowRelayServer(HttpRelay.Config config, PhysicalSourceStaticConfig [] pConfigs)
  throws IOException, InvalidConfigException,  DatabusException
  {
    this(config.build(), pConfigs);
  }

  public BizFollowRelayServer(HttpRelay.StaticConfig config, PhysicalSourceStaticConfig [] pConfigs)
  throws IOException, InvalidConfigException, DatabusException
  {
    super(config, pConfigs);
    initializeBizFollowCommandProcessors();
  }

  protected void initializeBizFollowCommandProcessors() throws DatabusException
  {
    RequestProcessorRegistry processorRegistry = getProcessorRegistry();
    DatabusEventProducer bizfollowEventProducer =
        new DatabusEventBizFollowRandomProducer(getEventBuffer(), 10, 100,
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

    config.setSourceName(String.valueOf(BIZFOLLOW_SRC_ID), FULLY_QUALIFIED_BIZFOLLOW_EVENT_NAME);

    HttpRelay.StaticConfig staticConfig = staticConfigLoader.loadConfig(startupProps);
    LOG.info("source = " + staticConfig.getSourceIds());

    BizFollowRelayServer serverContainer = new BizFollowRelayServer(staticConfig, null);
    serverContainer.startAndBlock();
  }
}
