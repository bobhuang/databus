package com.linkedin.databus2.test.integ;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.DatabusHttpClientImpl.RuntimeConfigBuilder;
import com.linkedin.databus.client.pub.DatabusClientException;
import com.linkedin.databus.client.pub.ServerInfo.ServerInfoBuilder;
import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.relay.member2.DatabusEventProfileRandomProducer;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.relay.config.LogicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.NoSuchSchemaException;
import com.linkedin.databus2.schemas.SourceIdNameRegistry;
import com.linkedin.databus2.schemas.utils.Utils;
import com.linkedin.databus3.espresso.EspressoRelay;
import com.linkedin.databus3.espresso.EspressoRelayFactory;
import com.linkedin.databus3.espresso.client.DatabusHttpV3ClientImpl;
import com.linkedin.databus3.espresso.schema.EspressoBackedSchemaRegistryService;
import com.linkedin.databus3.espresso.schema.EspressoSchemaName;
import com.linkedin.espresso.schema.SchemaRegistry;

public class TestRelayUtils
{

  private static EspressoRelayTestCluster _testCluster;

  public static EspressoRelayTestCluster getEspressoRelayTestCluster() throws IOException {
    if(_testCluster == null)
      _testCluster = new TestRelayUtils.EspressoRelayTestCluster();

    return _testCluster;
  }

  /**
   * for tests running under integration-test/integration-test-integ-ng
   * @param pathFromTop
   * @return
   */
  public static String buildPathForIntegTest(String pathFromTop) {
    // if the value for this prop is not set - I assume we are in eclipse, so we need to use "../.."
    String userDir = System.getProperties().getProperty("databus.basedir.path", "../../");

    return userDir + pathFromTop;
  }

  public static class EspressoRelayTestCluster {

    public Logger LOG = Logger.getLogger(TestRelayUtils.class); // can be overwritten

    private final String SCHEMA_ROOTDIR_PROP_NAME = "espresso.schema.rootdir";
    private final String DEFAULT_ESPRESSO_SCHEMA_ROOTDIR = "databus3-relay/databus3-relay-cmdline-pkg/espresso_schemas_registry";

    public final String PHYSICAL_SOURCE_URI = "pSourceUir";

    private final int _relayPort = 25001;
    private PhysicalSourceStaticConfig [] _pSConfigs;
    private DatabusHttpV3ClientImpl _client;
    private SourceIdNameRegistry _sourceIdNameRegistry;
    private EspressoRelay _realRelay;
    private EspressoBackedSchemaRegistryService _schemaRegistry;
    private EspressoRelay.StaticConfig _espressoRelayStaticConfig;


    private EspressoRelay.StaticConfigBuilder _cfgBuilder;
    private DatabusHttpV3ClientImpl.StaticConfigBuilder _clientCfgBuilder;

    //private Schema _schema;

    // list of logical sources from the registry
    private final List<LogicalSourceConfig> _lSources = new ArrayList<LogicalSourceConfig>(10);

    public EspressoRelayTestCluster() throws IOException {
      initConfigBuilders();
    }

    // create nessesary configBuilders
    private void initConfigBuilders() throws IOException {
      _clientCfgBuilder = new DatabusHttpV3ClientImpl.StaticConfigBuilder();

      // relay config
      _cfgBuilder = new EspressoRelay.StaticConfigBuilder();

      _cfgBuilder.getEventBuffer().setMaxSize(10*1024*1024);
      _cfgBuilder.getEventBuffer().setScnIndexSize(2*1024*1024);
      //cfgBuilder.getEventBuffer().setMaxIndividualBufferSize(1024*1024);
      _cfgBuilder.getEventBuffer().setAllocationPolicy("MMAPPED_MEMORY");
      _cfgBuilder.getEventBuffer().setMmapDirectory("/tmp/mmap");
      _cfgBuilder.getEventBuffer().getTrace().setOption("file");
      _cfgBuilder.getEventBuffer().getTrace().setAppendOnly(false);
      _cfgBuilder.getContainer().setHttpPort(_relayPort);
      _cfgBuilder.getContainer().getJmx().setJmxServicePort(_relayPort+1);


      // now client
      _clientCfgBuilder = new DatabusHttpV3ClientImpl.StaticConfigBuilder();

      _clientCfgBuilder.getCheckpointPersistence().setClearBeforeUse(true);
      _clientCfgBuilder.getContainer().getJmx().setJmxServicePort(_relayPort+2);
      _clientCfgBuilder.getClusterManager().setEnabled(false);

      //_clientCfgBuilder.getLoggingListener().getRuntime().setEnabled(true);
      //_clientCfgBuilder.getLoggingListener().getRuntime().setVerbosity("ALL");
      //_clientCfgBuilder.getLoggingListener().setLogTypedValue(true);
      _clientCfgBuilder.getConnectionDefaults().getEventBuffer().getTrace().setAppendOnly(false);
      _clientCfgBuilder.getConnectionDefaults().getEventBuffer().getTrace().setFilename("/tmp/client.trace");
      _clientCfgBuilder.getConnectionDefaults().getEventBuffer().getTrace().setOption("file");

    }

    /**
     * add your own config args before the relay is created
     * @return the relay config builder
     */
    public EspressoRelay.StaticConfigBuilder getEspressoRelayConfigBuilder() {
      return _cfgBuilder;
    }
    /**
     * add your own config args before the client is created
     * @return the client config builder
     */
    public DatabusHttpV3ClientImpl.StaticConfigBuilder getDatabusHttpV3ClientImplStaticConfigBuilder() {
      return _clientCfgBuilder;
    }

    /**
     * get schema for the db and table
     * @param dbName
     * @param tableName
     * @return
     * @throws NoSuchSchemaException
     * @throws DatabusException
     */
    public Schema getSchema(String dbName, String tableName) throws NoSuchSchemaException, DatabusException {
      EspressoSchemaName schemaName = EspressoSchemaName.create(dbName, tableName);

      Assert.assertNotNull(_schemaRegistry);
      return _schemaRegistry.fetchLatestSchemaObjByType(schemaName.getDatabusSourceName());

    }


    /**
     * overrite logger to use
     * @param log
     */
    public void setLogger(Logger log) {
      LOG = log;
    }

    public short getSourceIdForTable(String dbName, String tableName) throws InvalidConfigException {
      String srcName = dbName + "." + tableName; // TODO use EspressoName
      if(_lSources == null) {
        throw new InvalidConfigException("list of sources is not initialized yet");
      }
      for(LogicalSourceConfig lsc: _lSources) {
        if(lsc.getName().equals(srcName))
          return lsc.getId();
      }

      throw new InvalidConfigException("cannot find source id for " + tableName);
    }

    public  EspressoRelay.StaticConfigBuilder getRelayConfig() throws IOException {
      if(_cfgBuilder == null) {

      }
      return _cfgBuilder;
    }



    public EspressoBackedSchemaRegistryService createTestEspressoBackedSchemaRegistryService(String dbName)
        throws InvalidConfigException, DatabusException, com.linkedin.espresso.common.config.InvalidConfigException {
      //
      // TODO create a MOCK object for espresso registry!!
      //
      String rootDir = System.getProperty(SCHEMA_ROOTDIR_PROP_NAME);
      if(rootDir == null)
        rootDir = buildPathForIntegTest(DEFAULT_ESPRESSO_SCHEMA_ROOTDIR);
      LOG.info("Using espresso schema registry root: " + rootDir);


      // create schema registry and IdNameRegistry
      _sourceIdNameRegistry = new SourceIdNameRegistry();
      SchemaRegistry.Config configBuilder = new SchemaRegistry.Config();

      configBuilder.setMode("file");
      configBuilder.setRootSchemaNamespace(rootDir);

      _schemaRegistry = new EspressoBackedSchemaRegistryService(configBuilder.build(), _sourceIdNameRegistry);
      _schemaRegistry.loadAllSourcesOnlyIfNewEspressoDB(dbName);


      // for each logical source in the database create a logical source object
      for(String source :_schemaRegistry.getAllLogicalSourcesInDatabase(dbName)) {
        short id = _sourceIdNameRegistry.getSourceId(source).shortValue();
        LogicalSourceConfig lsc = new LogicalSourceConfig();
        lsc.setId(id);
        lsc.setName(source);
        lsc.setUri("uri");
        lsc.setPartitionFunction("someF");
        _lSources.add(lsc);

      }

      return _schemaRegistry;
    }


    public PhysicalSourceStaticConfig [] getPhysicalSourceStaticConfigs() {
      Assert.assertNotNull(_pSConfigs);
      return _pSConfigs;
    }

    /**
     * create test Espresso relay
     * @param dbName - db name
     * @param partNum - number of partitions
     * @param config - config??
     * @return
     * @throws IOException
     * @throws com.linkedin.espresso.common.config.InvalidConfigException
     * @throws DatabusException
     */
    public EspressoRelay createTestEspressoRelay(String dbName, int partNum)
        throws IOException, DatabusException, com.linkedin.espresso.common.config.InvalidConfigException {

      // create espressobased schema registry
      createTestEspressoBackedSchemaRegistryService(dbName);
      Assert.assertNotNull(_schemaRegistry);
      Assert.assertNotNull(_lSources);

      // create physical configs
      PhysicalSourceConfig [] pConfigs = new PhysicalSourceConfig[partNum];

      //create physical configuration and update it with the sources
      for(int i=0; i<pConfigs.length; i++) {
        PhysicalSourceConfig pConfig = new PhysicalSourceConfig(dbName, PHYSICAL_SOURCE_URI, i+100);
        for(LogicalSourceConfig lsc: _lSources)
          lsc.setPartition((short)pConfig.getId()); // in espresso pPartId matches lPartId of the source
            pConfig.setSources(_lSources);
            pConfigs[i] = pConfig;
      }



      _espressoRelayStaticConfig = getRelayConfig().build();
      EspressoRelayFactory factory = new EspressoRelayFactory(_cfgBuilder, null, _sourceIdNameRegistry);

      _pSConfigs =  new PhysicalSourceStaticConfig[pConfigs.length];
      int i=0;
      for(PhysicalSourceConfig pConfig : pConfigs) {
        _pSConfigs[i++] = pConfig.build();
      }
      _realRelay = factory.createEspressoRelayObject(_espressoRelayStaticConfig, _pSConfigs, _schemaRegistry);

      return _realRelay;
    }


    public  DatabusHttpV3ClientImpl createClient(String sources)
                        throws IOException, DatabusException, DatabusClientException {

      Assert.assertNotNull(_clientCfgBuilder);

      DatabusHttpV3ClientImpl.StaticConfig clientConf = _clientCfgBuilder.build();

      _client = new DatabusHttpV3ClientImpl(clientConf);
      //String sources = DB_NAME+ "." + DB_SCHEMA + ":" + PARTITION_ID;
      registerRelay(1, "relay1", new InetSocketAddress("localhost", _relayPort), sources, _client);

      return _client;
    }


    private void registerRelay(int id, String name, InetSocketAddress addr, String sources,
                                      DatabusHttpClientImpl client)
                                          throws InvalidConfigException
                                          {
      RuntimeConfigBuilder rtConfigBuilder =
          (RuntimeConfigBuilder)client.getClientConfigManager().getConfigBuilder();

      ServerInfoBuilder relayConfigBuilder = rtConfigBuilder.getRelay(Integer.toString(id));
      relayConfigBuilder.setName(name);
      relayConfigBuilder.setHost(addr.getHostName());
      relayConfigBuilder.setPort(addr.getPort());
      relayConfigBuilder.setSources(sources);

      client.getClientConfigManager().setNewConfig(rtConfigBuilder.build());
                                          }

    public void shutdown() {
      if(_client != null)
        _client.shutdown();

      if(_realRelay != null)
        _realRelay.shutdown();
    }


    public int addSomeEvents(int numEventsPerWin, int numWins, short srcId, PhysicalPartition p, Schema schema) {
      Assert.assertNotNull(_realRelay);
      int count = 0;
      DbusEventBufferAppendable buf = _realRelay.getEventBuffer().getDbusEventBufferAppendable(p);
      buf.start(1);
      LOG.info("for P=" + p + " got buffer " + buf.hashCode());

      byte[] schemaId = Utils.md5(schema.toString().getBytes());
      byte [] val = setDefaultValue(schema);
      short pPartId = p.getId().shortValue();
      short lPartId = pPartId;
      for(int j=0; j<numWins; j++) {
        buf.startEvents();

        for(int i=0; i<numEventsPerWin; i++) {
          assertTrue(buf.appendEvent(new DbusEventKey(j*10 + i + 1), pPartId, lPartId,
                                     System.currentTimeMillis() * 1000000, srcId,
                                     schemaId, val, false, null));
          count++;
        }
        buf.endEvents(10*(j + 1), null);
      }
      return count;
    }

    // put some default values
    // currently covers some types - NOT ALL
    private byte [] setDefaultValue(Schema s) {
      GenericRecord r = new GenericData.Record(s);

      for(Field f : s.getFields()) {
        Schema.Type st = f.schema().getType();
        String name = f.name();
        //System.out.print("name=" + name + "; type=" + st + ": ");
        if(st.equals(Schema.Type.UNION)) {
          Schema [] as = new Schema[10];
          f.schema().getTypes().toArray(as);
          // System.out.print(Arrays.toString(as));
          if(as[0].equals(Schema.Type.NULL))
            r.put(name, null);
          // else !!!!!?????
        } else if(st.equals(Schema.Type.LONG))
          r.put(name, 100L);
        else if(st.equals(Schema.Type.INT))
          r.put(name, 200);
        else if(st.equals(Schema.Type.BOOLEAN))
          r.put(name, true);
        else if(st.equals(Schema.Type.BYTES))
          r.put(name, new byte[10]);
        else if(st.equals(Schema.Type.STRING))
          r.put(name, "stringVal");

        //System.out.println();
      }

      return DatabusEventProfileRandomProducer.serializeEvent(s, r);
    }

  }

  public PhysicalSourceConfig convertFromJsonToPhysicalSourceConfig(String str) {
    ObjectMapper _mapper = new ObjectMapper();
    InputStreamReader isr = new InputStreamReader(IOUtils.toInputStream(str));
    PhysicalSourceConfig pConfig = null;
    try
    {
      pConfig = _mapper.readValue(isr, PhysicalSourceConfig.class);
    }
    catch (JsonParseException e) {
      fail("Failed parsing", e);
    }
    catch (JsonMappingException e){
      fail("Failed parsing", e);
     }
    catch (IOException e){
      fail("Failed parsing", e);
     }

    try
    {
      isr.close();
    }
    catch (IOException e)
    {
      fail("Failed",e);
    }
    return pConfig;
  }




}
