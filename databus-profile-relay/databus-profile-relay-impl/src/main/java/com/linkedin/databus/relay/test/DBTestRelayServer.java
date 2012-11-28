package com.linkedin.databus.relay.test;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.container.request.OracleProducerTestRequestProcessor;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.netty.ServerContainer;
import com.linkedin.databus2.core.container.request.RequestProcessorRegistry;
import com.linkedin.databus2.producers.EventCreationException;
import com.linkedin.databus2.producers.EventProducer;
import com.linkedin.databus2.producers.db.OracleEventProducer;
import com.linkedin.databus2.producers.db.OracleTxlogEventReader;
import com.linkedin.databus2.producers.db.SourceDBEventReader;
import com.linkedin.databus2.relay.DatabusRelayMain;
import com.linkedin.databus2.relay.config.LogicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;

public class DBTestRelayServer extends DatabusRelayMain 
{

	public DBTestRelayServer(StaticConfig config,
			PhysicalSourceStaticConfig[] pConfigs) throws IOException,
			InvalidConfigException, DatabusException {
		super(config, pConfigs);
	}

	public void addOneProducer(PhysicalSourceStaticConfig pConfig)
			throws DatabusException, EventCreationException, UnsupportedKeyException,
			SQLException, InvalidConfigException 
	{
		super.addOneProducer(pConfig);

		PhysicalPartition pPartition = pConfig.getPhysicalPartition();
		EventProducer producer = _producers.get(pPartition);
		LOG.info("Add One Producer called !!");
		if (producer instanceof OracleEventProducer)
		{
			SourceDBEventReader reader = ((OracleEventProducer)producer).getSourceDBReader();

			if (reader instanceof OracleTxlogEventReader)
			{
				LOG.info("Adding OracleProducerTestRequestProcessor");
				RequestProcessorRegistry processorRegistry = getProcessorRegistry();
				processorRegistry.reregister(OracleProducerTestRequestProcessor.COMMAND_NAME, 
						new OracleProducerTestRequestProcessor((OracleTxlogEventReader)reader, null));
			}
		}
	}

	/**
	 * @param args
	 */
	 public static void main(String[] args)
			 throws Exception
	 {
		 String [] leftOverArgs = processLocalArgs(args);

		 // Process the startup properties and load configuration
		 Properties startupProps = ServerContainer.processCommandLineArgs(leftOverArgs);
		 Config config = new Config();
		 ConfigLoader<StaticConfig> staticConfigLoader = new ConfigLoader<StaticConfig>("databus.relay.", config);

		 // read physical config files
		 ObjectMapper mapper = new ObjectMapper();
		 PhysicalSourceConfig [] physicalSourceConfigs = new PhysicalSourceConfig[_dbRelayConfigFiles.length];
		 PhysicalSourceStaticConfig [] pStaticConfigs =
				 new PhysicalSourceStaticConfig[physicalSourceConfigs.length];

		 int i = 0;
		 for(String file : _dbRelayConfigFiles) {
			 LOG.info("processing file: " + file);
			 File sourcesJson = new File(file);
			 PhysicalSourceConfig pConfig = mapper.readValue(sourcesJson, PhysicalSourceConfig.class);
			 pConfig.checkForNulls();
			 physicalSourceConfigs[i] = pConfig;
			 pStaticConfigs[i] = pConfig.build();

			 // Register all sources with the static config
			 for(LogicalSourceConfig lsc : pConfig.getSources()) {
				 config.setSourceName("" + lsc.getId(), lsc.getName());
			 }
			 i++;
		 }

		 HttpRelay.StaticConfig staticConfig = staticConfigLoader.loadConfig(startupProps);

		 // Create and initialize the server instance
		 DatabusRelayMain serverContainer = new DBTestRelayServer(staticConfig, pStaticConfigs);

		 serverContainer.initProducers();
		 serverContainer.startAndBlock();
	}
}
