package com.linkedin.databus3.espresso;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.container.request.RplDbusManagerAdminRequestProcessor;
import com.linkedin.databus.container.request.tcp.GetLastSequenceRequest;
import com.linkedin.databus.container.request.tcp.SendEventsRequest;
import com.linkedin.databus.container.request.tcp.StartSendEventsRequest;
import com.linkedin.databus.core.EventLogReader;
import com.linkedin.databus.core.EventLogWriter;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.StatsCollectors;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.DatabusEventRandomProducer;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.PhysicalSourceConfigBuilder;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.monitoring.mbean.HttpStatisticsCollector;
import com.linkedin.databus2.core.container.request.CommandsRegistry;
import com.linkedin.databus2.core.seq.MultiServerSequenceNumberHandler;
import com.linkedin.databus2.core.seq.SequenceNumberHandlerFactory;
import com.linkedin.databus2.relay.config.DataSourcesStaticConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.SchemaRegistryStaticConfig;
import com.linkedin.databus2.schemas.SourceIdNameRegistry;
import com.linkedin.databus3.espresso.cmclient.ClusterManagerPhysicalSourceConfigBuilder;
import com.linkedin.databus3.espresso.cmclient.StorageRelayClusterManagerStaticConfig;
import com.linkedin.databus3.espresso.cmclient.StorageRelayClusterManagerStaticConfigBuilder;
import com.linkedin.databus3.espresso.schema.EspressoBackedSchemaRegistryService;
import com.linkedin.databus3.rpl_dbus_manager.RplDbusAdapter;
import com.linkedin.databus3.rpl_dbus_manager.RplDbusException;
import com.linkedin.databus3.rpl_dbus_manager.RplDbusManager;
import com.linkedin.databus3.rpl_dbus_manager.RplDbusManagerConfigBuilder;
import com.linkedin.databus3.rpl_dbus_manager.RplDbusManagerStaticConfig;
import com.linkedin.databus3.rpl_dbus_manager.monitoring.mbean.RplDbusTotalStats;
import com.linkedin.espresso.schema.SchemaRegistry;


public class EspressoRelay extends HttpRelay
{

  private final SequenceNumberHandlerFactory _handlerFactory;
  private final MultiServerSequenceNumberHandler _multiServerSeqHandler;
  private static final Logger LOG = Logger.getLogger(EspressoRelay.class.getName());
  private RplDbusManager _rplDbusManager = null;
  private ClusterManagerPhysicalSourceConfigBuilder _cmConnector = null;
  
  public static class StaticConfig extends HttpRelay.StaticConfig
  {
    private final SchemaRegistry.StaticConfig _espressoSchemas;
    private final StorageRelayClusterManagerStaticConfig _clusterManagerStaticConfig;
    private final RplDbusManagerStaticConfig _rplDbusManagerConfig;
    private final String _espressoDBs;

    public StaticConfig(com.linkedin.databus.core.DbusEventBuffer.StaticConfig eventBufferConfig,
                        com.linkedin.databus2.core.container.netty.ServerContainer.StaticConfig containerConfig,
                        SchemaRegistryStaticConfig schemaRegistryConfig,
                        List<IdNamePair> sourceIds,
                        RuntimeConfigBuilder runtime,
                        HttpStatisticsCollector.StaticConfig httpStatsCollector,
                        DbusEventsStatisticsCollector.StaticConfig inboundEventsStatsCollector,
                        DbusEventsStatisticsCollector.StaticConfig outboundEventsStatsCollector,
                        DatabusEventRandomProducer.StaticConfig randomProducer,
                        EventLogWriter.StaticConfig eventLogWriterConfig,
                        EventLogReader.StaticConfig eventLogReaderConfig,
                        boolean startDbPuller,
                        DataSourcesStaticConfig dataSources,
                        SchemaRegistry.StaticConfig espressoSchemas,
                        StorageRelayClusterManagerStaticConfig clusterManagerStaticConfig,
                        RplDbusManagerStaticConfig rplDbusManagerStaticConfig,
                        String espressoDBs,
                        PhysicalSourceStaticConfig[] physicalSourcesConfigs
                        )
    {
      super(eventBufferConfig,
            containerConfig,
            schemaRegistryConfig,
            sourceIds,
            runtime,
            httpStatsCollector,
            inboundEventsStatsCollector,
            outboundEventsStatsCollector,
            randomProducer,
            eventLogWriterConfig,
            eventLogReaderConfig,
            startDbPuller,
            dataSources,
            physicalSourcesConfigs
            );
      _espressoSchemas = espressoSchemas;
      _clusterManagerStaticConfig = clusterManagerStaticConfig;
      _rplDbusManagerConfig = rplDbusManagerStaticConfig;
      _espressoDBs = espressoDBs;
    }

    public RplDbusManagerStaticConfig getRplDbusManager() {
      return _rplDbusManagerConfig;
    }

    /** The configuration for the espresso schema registry */
    public SchemaRegistry.StaticConfig getEspressoSchemas()
    {
      return _espressoSchemas;
    }

    /** The configuration for the espresso schema registry */
    public StorageRelayClusterManagerStaticConfig getClusterManager()
    {
      return _clusterManagerStaticConfig;
    }

    public String getEspressoDBs()
    {
      return _espressoDBs;
    }

  }

  public static class StaticConfigBuilder extends StaticConfigBuilderBase
                                          implements ConfigBuilder<StaticConfig>
  {
    private final SchemaRegistry.Config _espressoSchemas;
    private final StorageRelayClusterManagerStaticConfigBuilder _clusterManagerStaticConfigBuilder;
    private final RplDbusManagerConfigBuilder _rplDbusManagerConfigBuilder;
    private String _espressoDBs;

    public StaticConfigBuilder() throws IOException
    {
      super();
      _espressoSchemas = new SchemaRegistry.Config();
      _clusterManagerStaticConfigBuilder = new StorageRelayClusterManagerStaticConfigBuilder();
      _rplDbusManagerConfigBuilder = new RplDbusManagerConfigBuilder();
    }

    public SchemaRegistry.Config getEspressoSchemas()
    {
      return _espressoSchemas;
    }

    public StorageRelayClusterManagerStaticConfigBuilder getClusterManager()
    {
      return _clusterManagerStaticConfigBuilder;
    }

    public RplDbusManagerConfigBuilder getRplDbusManager() {
      return _rplDbusManagerConfigBuilder;
    }

    @Override
    public StaticConfig build() throws InvalidConfigException
    {
      ArrayList<IdNamePair> sourceIds = new ArrayList<IdNamePair>(_sourceName.size());
      for (String srcIdStr: _sourceName.keySet())
      {
        try
        {
          long srcId = Long.parseLong(srcIdStr);
          sourceIds.add(new IdNamePair(srcId, _sourceName.get(srcIdStr)));
        }
        catch (NumberFormatException nfe)
        {
          throw new InvalidConfigException("Invalid source id: " + srcIdStr);
        }

      }

      SchemaRegistry.StaticConfig espressoSchemasConf = null;

      try
      {
        espressoSchemasConf = _espressoSchemas.build();
      }
      catch (com.linkedin.espresso.common.config.InvalidConfigException ice)
      {
        throw new InvalidConfigException(ice);
      }

      PhysicalSourceStaticConfig[] physConfigs = buildInitPhysicalSourcesConfigs();

      // TODO (DDSDBUS-75) Add config verification
      return new StaticConfig(_eventBuffer.build(), _container.build(), _schemaRegistry.build(),
                              sourceIds, getRuntime(), _httpStatsCollector.build(),
                              _inboundEventsStatsCollector.build(),
                              _outboundEventsStatsCollector.build(),
                              _randomProducer.build(),
                              _eventLogWriter.build(),
                              _eventLogReader.build(),
                              Boolean.parseBoolean(_startDbPuller),
                              _dataSources.build(),
                              espressoSchemasConf,
                              _clusterManagerStaticConfigBuilder.build(),
                              _rplDbusManagerConfigBuilder.build(),
                              _espressoDBs,
                              physConfigs
      						);
    }

    public String getEspressoDBs()
    {
      return _espressoDBs;
    }

    public void setEspressoDBs(String espressoDBs)
    {
      _espressoDBs = espressoDBs;
    }

  }

  EspressoRelay(StaticConfig config, PhysicalSourceStaticConfig [] pConfigs,
                SourceIdNameRegistry sourcesIdNameRegistry,
                EspressoBackedSchemaRegistryService schemaRegistry)
  throws IOException, InvalidConfigException, DatabusException
  {
    super(config, pConfigs, sourcesIdNameRegistry, schemaRegistry);
    _handlerFactory = config.getDataSources().getSequenceNumbersHandler().createFactory();
    _multiServerSeqHandler = new MultiServerSequenceNumberHandler(_handlerFactory);

    initRplDbusManager(config.getRplDbusManager());
    
    if (config.getClusterManager().getEnabled())
    {
    	initRelayAdapter(config, sourcesIdNameRegistry, schemaRegistry);
    }
    initializeEspressoRelayCommandProcessors();
  }

  protected void initRplDbusManager(RplDbusManagerStaticConfig config)
                                        throws IOException, DatabusException {
    if(config == null || !config.getEnabled()) {
      LOG.warn("RplDbusManager is disabled");
      _rplDbusManager = null; // final field;
      return;
    }

    try {
      _rplDbusManager = new RplDbusManager(config, _multiServerSeqHandler);
      _rplDbusManager.setMBeanServer(getMbeanServer());
      _rplDbusManager.configureRplDbusTcpPort(_relayStaticConfig.getContainer().getTcp().getPort());

      _rplDbusManager.start();
    } catch (RplDbusException e) {
        throw new InvalidConfigException("cannot initialize enabled RplDbusManager", e);
    }
  }

  protected void initRelayAdapter(StaticConfig config, SourceIdNameRegistry sourcesIdNameRegistry,
		  						  EspressoBackedSchemaRegistryService schemaRegistry)
  throws IOException, DatabusException 
  {
	  _cmConnector = new ClusterManagerPhysicalSourceConfigBuilder(config, sourcesIdNameRegistry, schemaRegistry, this);
	  _cmConnector.connectToCM();
  }


  /**
   *
   * @return rplDbusManager for this Relay
   */
  public RplDbusManager getRplDbusManager() {
    return _rplDbusManager;
  }

  protected void initializeEspressoRelayCommandProcessors() throws DatabusException
  {
    CommandsRegistry commandsRegistry = getCommandsRegistry();

    commandsRegistry.registerCommand(
        null, GetLastSequenceRequest.OPCODE,
        new GetLastSequenceRequest.BinaryParserFactory(),
        new GetLastSequenceRequest.ExecHandlerFactory(getEventBuffer(),
                                                      _multiServerSeqHandler));

    SendEventsRequest.ExecHandlerFactory sendEventsExecFactory =
        new SendEventsRequest.ExecHandlerFactory(getEventBuffer(), getRelayStaticConfig(),
                                                 _multiServerSeqHandler,
                                                 getSourcesIdNameRegistry(),
                                                 getInBoundStatsCollectors(),
                                                 getOutBoundStatsCollectors(),
                                                 getSchemaRegistryService(), this);
    commandsRegistry.registerCommand(null, StartSendEventsRequest.OPCODE,
                                     new StartSendEventsRequest.BinaryParserFactory(),
                                     sendEventsExecFactory);
    commandsRegistry.registerCommand(null, SendEventsRequest.OPCODE,
                                     new SendEventsRequest.BinaryParserFactory(),
                                     sendEventsExecFactory);

    // RplDbusState processor
    if(_rplDbusManager != null)
    	_processorRegistry.register(RplDbusManagerAdminRequestProcessor.COMMAND_NAME,
                                new RplDbusManagerAdminRequestProcessor(null, _rplDbusManager));

  }

  @Override
  protected void doStart()
  {
	  super.doStart();
  }

  @Override
  public int[] getBinlogOffset(int serverId)
  throws DatabusException
  {
    long offset = _rplDbusManager.figureOutConnectionFileAndOffset(serverId, false);
    if (offset == -1L)
    {
      throw new DatabusException("Could not get offset for server ID" + serverId);
    }
    int fileId = RplDbusAdapter.getFileNameIndexFromBinlogOffset(offset);
    int offsetInFile = RplDbusAdapter.getFileOffsetFromBinlogOffset(offset);
    return new int[]{fileId, offsetInFile};
  }

  // DDSDBUS-1171 has the specs
  // Method to print some terse information about the relay. We should keep this terse
  // and easily parsable via standard unix commands.
  @Override
  public Map<String,String> printInfo()
  throws DatabusException
  {
    if (!(_relayStaticConfig instanceof StaticConfig))
    {
      throw new DatabusException("Unknown instance of static configuration");
    }

    StaticConfig espressoRelayStaticConfig = (StaticConfig)_relayStaticConfig;

    HashMap<String, String> map =  new HashMap<String, String>();

    map.put("espressoSchemas.zkAddress",
        espressoRelayStaticConfig._espressoSchemas.getZkAddress());
    map.put("clusterManager.storageZkConnectString",
        espressoRelayStaticConfig.getClusterManager().getStorageZkConnectString());
    map.put("clusterManager.relayZkConnectString",
        espressoRelayStaticConfig.getClusterManager().getRelayZkConnectString());

    // Cluster Names
    map.put("clusterManager.storageClusterName",
        espressoRelayStaticConfig.getClusterManager().getStorageClusterName());
    map.put("clusterManager.relayClusterName",
        espressoRelayStaticConfig.getClusterManager().getRelayClusterName());
    map.put("rplDbusManager.relayClusterName",
        espressoRelayStaticConfig.getRplDbusManager().getRelayClusterName());

    // Instance names
    map.put("clusterManager.instanceName",
        espressoRelayStaticConfig.getClusterManager().getInstanceName());
    map.put("rplDbusManager.instanceName",
        espressoRelayStaticConfig.getRplDbusManager().getInstanceName());

    // Relay to rpldbusMapping
    map.put("rplDbusManager.relayRplDbusMapping",
        espressoRelayStaticConfig.getRplDbusManager().getRelayRplDbusMapping());
    return map;
  }

  @Override
  protected void doShutdown()
  {
	  super.doShutdown();
	  if(_rplDbusManager != null)
	    _rplDbusManager.shutdown();
	  if(_cmConnector != null)
		_cmConnector.disconnectFromCM();

	  if (null != getSchemaRegistryService() &&
	      getSchemaRegistryService() instanceof EspressoBackedSchemaRegistryService)
	  {
	    LOG.info("stopping espresso backed schema registry refresh thread");
	    ((EspressoBackedSchemaRegistryService)getSchemaRegistryService()).shutdown();
	    LOG.info("espresso backed schema registry refresh thread stopped.");
	  }
  }

  public static void main(String[] args) throws Exception
  {
    Cli cli = new Cli();
    cli.processCommandLineArgs(args);

    Properties startupProps = cli.getConfigProps();

    StaticConfigBuilder config = new StaticConfigBuilder();

    ConfigLoader<StaticConfig> staticConfigLoader =
        new ConfigLoader<StaticConfig>("databus.relay.", config);

    EspressoRelay.StaticConfig staticConfig = staticConfigLoader.loadConfig(startupProps);

    PhysicalSourceStaticConfig [] pConfigs = null;
    if (! staticConfig.getClusterManager().getEnabled())
    {
      if (null != cli.getPhysicalSrcConfigFiles())
      {
    	LOG.info("ClusterManager integration is not enabled and found physical source config files to create PSSC");
        PhysicalSourceConfigBuilder psourceConfBuilder =
            new PhysicalSourceConfigBuilder(cli.getPhysicalSrcConfigFiles());
        pConfigs = psourceConfBuilder.build();
      }
      else
      {
      	LOG.info("phy source config files not specified");
      }
    }

    EspressoRelayFactory relayFactory = new EspressoRelayFactory(config, pConfigs);
    LOG.info("CONFIG for RPLDBUS (mysqlportmapping) = " + config.getRplDbusManager().getmysqlPortMapping());
    LOG.info("CONFIG for MAXSCNHANDLER = " + config.getDataSources().getSequenceNumbersHandler().getFile().getKey() +
             "; file=" + config.getDataSources().getSequenceNumbersHandler().getFile().getScnDir());
    HttpRelay relay = relayFactory.createRelay();

    LOG.info("source = " + staticConfig.getSourceIds());
    try
    {
      relay.registerShutdownHook();
      relay.startAndBlock();
    }
    catch (Exception e)
    {
      LOG.error("Error starting the relay", e);
    }
    LOG.info("Exiting relay");
  }

  public StatsCollectors<RplDbusTotalStats> getRplDbusStatsCollectors() {
    if(_rplDbusManager == null)
      return null;

    return _rplDbusManager.getRplDbusStatsCollectors();
  }
}
