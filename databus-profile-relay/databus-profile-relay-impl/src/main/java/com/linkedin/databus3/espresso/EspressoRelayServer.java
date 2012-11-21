package com.linkedin.databus3.espresso;

import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.container.request.GenerateDataEventsRequestProcessor;
import com.linkedin.databus.container.request.LoadDataEventsRequestProcessor;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.DatabusEventProducer;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.PhysicalSourceConfigBuilder;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.request.RequestProcessorRegistry;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.SourceIdNameRegistry;
import com.linkedin.databus3.espresso.schema.EspressoBackedSchemaRegistryService;


public class EspressoRelayServer extends EspressoRelay
{
    private static final Logger LOG = Logger.getLogger(EspressoRelayServer.class.getName());

	EspressoRelayServer(StaticConfig config, PhysicalSourceStaticConfig [] pConfigs,
            SourceIdNameRegistry sourcesIdNameRegistry, EspressoBackedSchemaRegistryService schemaRegistry)
    throws IOException, InvalidConfigException, DatabusException
	{
		super(config, pConfigs,sourcesIdNameRegistry, schemaRegistry);
		initializeEspressoRelayCommandProcessorsRandom();
	}

	protected void initializeEspressoRelayCommandProcessorsRandom() throws DatabusException
	{
		RequestProcessorRegistry processorRegistry = getProcessorRegistry();
		DatabusEventProducer espressoEventProducer = new DatabusEventEspressoRandomProducer(getEventBuffer(), 10, 100,
				getRelayStaticConfig().getSourceIds(),
				getSchemaRegistryService(),
				getRelayStaticConfig().getRandomProducer());

		processorRegistry.register(
				GenerateDataEventsRequestProcessor.COMMAND_NAME,
				new GenerateDataEventsRequestProcessor(null,
						this,
						espressoEventProducer));
		processorRegistry.register(
				LoadDataEventsRequestProcessor.COMMAND_NAME,
				new LoadDataEventsRequestProcessor(getDefaultExecutorService(), this));
	}
  
	// factory
	//
	public static class EspressoRelayServerFactory extends EspressoRelayFactory {

		public EspressoRelayServerFactory(
				EspressoRelay.StaticConfigBuilder config,
				PhysicalSourceStaticConfig[] fallbackPhysicalSrcConfigs) {
			super(config, fallbackPhysicalSrcConfigs);
		}

		@Override
		public EspressoRelay createEspressoRelayObject(EspressoRelay.StaticConfig relayConfig, PhysicalSourceStaticConfig[] pConfigs, 
				EspressoBackedSchemaRegistryService regService) throws InvalidConfigException, DatabusException {
			try
			{
				return new EspressoRelayServer(relayConfig, pConfigs, getSourcesIdNameRegistry(), regService);
			}
			catch (IOException e)
			{
				throw new DatabusException("error creating a relay", e);
			}
		}
	}

	public static void main(String[] args) throws Exception
	{
		Cli cli = new Cli();
		cli.processCommandLineArgs(args);

		Properties startupProps = cli.getConfigProps();

		EspressoRelay.StaticConfigBuilder config = new EspressoRelay.StaticConfigBuilder();

		ConfigLoader<EspressoRelay.StaticConfig> staticConfigLoader =
				new ConfigLoader<EspressoRelay.StaticConfig>("databus.relay.", config);

		EspressoRelay.StaticConfig staticConfig = staticConfigLoader.loadConfig(startupProps);

		PhysicalSourceStaticConfig [] pConfigs = null;
		if (! staticConfig.getClusterManager().getEnabled())
		{
			if (null != cli.getPhysicalSrcConfigFiles())
			{
				PhysicalSourceConfigBuilder psourceConfBuilder =
						new PhysicalSourceConfigBuilder(cli.getPhysicalSrcConfigFiles());
				pConfigs = psourceConfBuilder.build();
			}
		}

		EspressoRelayServerFactory relayFactory = new EspressoRelayServerFactory(config, pConfigs);
		HttpRelay relay = relayFactory.createRelay();

		LOG.info("source = " + staticConfig.getSourceIds());
		try
		{
			relay.startAndBlock();
		}
		catch (Exception e)
		{
			LOG.error("Error starting the relay", e);
		}
		LOG.info("Exiting relay");
	}  
}
