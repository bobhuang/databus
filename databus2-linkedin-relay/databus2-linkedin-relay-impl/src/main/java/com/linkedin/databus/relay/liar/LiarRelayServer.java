package com.linkedin.databus.relay.liar;

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

public class LiarRelayServer extends HttpRelay
{
  public static final String MODULE = LiarRelayServer.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public LiarRelayServer() throws IOException, InvalidConfigException,
                                  DatabusException
  {
    this(new HttpRelay.Config(), null);
  }

  public LiarRelayServer(HttpRelay.Config config, PhysicalSourceStaticConfig [] pConfigs)
  throws IOException, InvalidConfigException, DatabusException
  {
    this(config.build(), pConfigs);
  }

  public LiarRelayServer(HttpRelay.StaticConfig config, PhysicalSourceStaticConfig [] pConfigs)
  throws IOException, InvalidConfigException, DatabusException
  {
    super(config, pConfigs);
    initializeLiarRelayCommandProcessors();
  }

  protected void initializeLiarRelayCommandProcessors() throws DatabusException
  {
    RequestProcessorRegistry processorRegistry = getProcessorRegistry();
    DatabusEventProducer liarEventProducer =
        new DatabusEventLiarRandomProducer(getEventBuffer(), 10, 100,
                                           getRelayStaticConfig().getSourceIds(),
                                           getSchemaRegistryService(),
                                           getRelayStaticConfig().getRandomProducer());

    processorRegistry.register(
        GenerateDataEventsRequestProcessor.COMMAND_NAME,
        new GenerateDataEventsRequestProcessor(null,
                                               this,
                                               liarEventProducer));
   processorRegistry.register(
       LoadDataEventsRequestProcessor.COMMAND_NAME,
       new LoadDataEventsRequestProcessor(getDefaultExecutorService(), this));
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception
  {
    Properties startupProps = HttpRelay.processCommandLineArgs(args);
    Config config = new Config();

    config.getContainer().setIdFromName("LiarRelayServer.localhost");

    ConfigLoader<StaticConfig> staticConfigLoader =
        new ConfigLoader<StaticConfig>("databus.relay.", config);

    String[] liarFQN = DatabusEventLiarRandomProducer.getFullyQualifiedEventNames();
    for (int srcInd = 0; srcInd < DatabusEventLiarRandomProducer._liarSrcIdList.length; srcInd++)
    {
      config.setSourceName(String.valueOf(DatabusEventLiarRandomProducer._liarSrcIdList[srcInd]),
                           liarFQN[srcInd]);
    }

    HttpRelay.StaticConfig staticConfig = staticConfigLoader.loadConfig(startupProps);
    LOG.info("source = " + staticConfig.getSourceIds());

    LiarRelayServer serverContainer = new LiarRelayServer(staticConfig, null);
    serverContainer.startAndBlock();
  }
}
