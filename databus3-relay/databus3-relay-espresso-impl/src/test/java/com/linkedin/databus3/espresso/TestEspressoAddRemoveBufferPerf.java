package com.linkedin.databus3.espresso;


import static org.testng.Assert.fail;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBufferMult;
import com.linkedin.databus.core.DbusEventBufferMult.PhysicalPartitionKey;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.cmclient.ResourceKey;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.data_model.PhysicalSource;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus.core.util.RngUtils;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.relay.config.LogicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.SourceIdNameRegistry;
import com.linkedin.databus3.espresso.EspressoRelay.StaticConfigBuilder;
import com.linkedin.databus3.espresso.cmclient.MockPhysicalSourceConfigBuilder;
import com.linkedin.databus3.espresso.schema.EspressoBackedSchemaRegistryService;
import com.linkedin.espresso.common.config.InvalidConfigException;
import com.linkedin.espresso.schema.SchemaRegistry;

@Test(singleThreaded=true)
public class TestEspressoAddRemoveBufferPerf
{
  private final static Logger LOG = Logger.getLogger(TestEspressoAddRemoveBufferPerf.class);

  static
  {
    //PatternLayout defaultLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c} %m%n");
    PatternLayout defaultLayout = new PatternLayout("%m%n");
    ConsoleAppender defaultAppender = new ConsoleAppender(defaultLayout);

    Logger.getRootLogger().removeAllAppenders();
    Logger.getRootLogger().addAppender(defaultAppender);

    Logger.getRootLogger().setLevel(Level.OFF);
    Logger.getRootLogger().setLevel(Level.ERROR);


    //Logger l = Logger.getLogger(FileBasedEventTrackingCallback.class.getName());
    //l.setLevel(Level.INFO);
    Logger.getRootLogger().setLevel(Level.INFO);
    //Logger.getRootLogger().setLevel(Level.DEBUG);
  }

  private EspressoRelay.StaticConfig _espressoRelayStaticConfig;
  private EspressoRelay _realRelay;
  private EspressoBackedSchemaRegistryService _schemaRegistry;
  private SourceIdNameRegistry _sourceIdNameRegistry;
  private long _baseMem;

  private final static String DB_NAME = "BizProfile"; //because we have to use a real registry - we have to use a real name :(
  private final static String SCHEMA_ROOTDIR_PROP_NAME = "espresso.schema.rootdir";
  private final static String DEFAULT_ESPRESSO_SCHEMA_ROOTDIR = "../databus3-relay-cmdline-pkg/espresso_schemas_registry";
  private static String SCHEMA_ROOTDIR;

  @BeforeTest
  public void setUp() throws InvalidConfigException, com.linkedin.databus.core.util.InvalidConfigException, DatabusException, IOException {

    SCHEMA_ROOTDIR = System.getProperty(SCHEMA_ROOTDIR_PROP_NAME, DEFAULT_ESPRESSO_SCHEMA_ROOTDIR);
    LOG.info("Using espresso schema registry root: " + SCHEMA_ROOTDIR);

    // create fake registries - we have to use a real espresso schema registry
    SchemaRegistry.Config configBuilder = new SchemaRegistry.Config();
    configBuilder.setMode("file");

    configBuilder.setRootSchemaNamespace(SCHEMA_ROOTDIR);
    configBuilder.setRefreshPeriodMs(10000000);

    SchemaRegistry.StaticConfig config = configBuilder.build();

    SourceIdNameRegistry sourceIdNameRegistry = new SourceIdNameRegistry();

    _schemaRegistry = new EspressoBackedSchemaRegistryService(config, sourceIdNameRegistry);
    List<IdNamePair> pairs = new ArrayList<IdNamePair>();
    long i = 0;
    for(String source :_schemaRegistry.getAllLogicalSourcesInDatabase(DB_NAME)) {
      pairs.add(new IdNamePair(i++, source));
    }
    _sourceIdNameRegistry = new SourceIdNameRegistry();
    _sourceIdNameRegistry.updateFromIdNamePairs(pairs);

    EspressoRelay.StaticConfigBuilder cfgBuilder = new StaticConfigBuilder();
    cfgBuilder.getEventBuffer().setMaxSize(10*1024*1024);
    cfgBuilder.getEventBuffer().setScnIndexSize(2*1024*1024);
    //cfgBuilder.getEventBuffer().setMaxIndividualBufferSize(1024*1024);
    cfgBuilder.getEventBuffer().setAllocationPolicy("MMAPPED_MEMORY");
    cfgBuilder.getEventBuffer().setMmapDirectory("/tmp/mmap");
    cfgBuilder.getEventBuffer().getTrace().setOption("file");
    cfgBuilder.getEventBuffer().getTrace().setAppendOnly(false);
    cfgBuilder.getClusterManager().setInstanceName("localhost_9092");
    _espressoRelayStaticConfig = cfgBuilder.build();
    _realRelay = new EspressoRelay(_espressoRelayStaticConfig, null, _sourceIdNameRegistry, _schemaRegistry);
  }

  //
  // This test add and removes buffers to the buffer mult
  // to check for memory leaks
  @Test
  public void addRemoveBuffersDirectlyUnit() throws Exception {
    addRemoveBuffersDirectly(10);
  }

  private void addRemoveBuffersDirectlyPerf() throws Exception {
    addRemoveBuffersDirectly(500);
  }

  //the actuall test
  private void addRemoveBuffersDirectly(int iterNum) throws Exception {
    short bufNum = 50;
    String pName = "part";
    String uri = "uri";
    if(_realRelay == null) fail("Real Espresso relay init failed");
    Runtime.getRuntime().gc();
    _baseMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    LOG.info("base_mem = " + _baseMem + ";mem=" + printMemory());

    try {
      for(short k = 0; k<iterNum; k++) {
        if(k%100 == 0)
          Runtime.getRuntime().gc();
        short startBuf =(short)(k*bufNum);
        short endBuf = (short)(startBuf + bufNum);
        for(short i=startBuf; i<endBuf; i++) {
          PhysicalSourceStaticConfig pStaticConfig = buildPhysicalSourceStaticConfig(i, pName, uri);
          DbusEventBuffer buf = _realRelay.addNewBuffer(pStaticConfig, _espressoRelayStaticConfig);
          addEvents(buf, 1000);
        }
        if(k%100==0)
          LOG.info(k + ":" + printMemory());

        for(short i=startBuf; i<endBuf; i++) {
          PhysicalSourceStaticConfig pConfig = buildPhysicalSourceStaticConfig(i, pName, uri);
          _realRelay.removeBuffer(pConfig);
        }
        if(k%100==0)
          LOG.info("r" + k + ":" + printMemory());
      }
    } catch (java.lang.OutOfMemoryError oome) {
      LOG.error("got exception:", oome);
      //String pid = new File("/proc/self").getCanonicalFile().getName();
      //LOG.info("sleeping" + pid);
      //Thread.sleep(10000); // needed for heap dump only
      //LOG.info("done");
    }
  }

  private void printAllBuffers(DbusEventBufferMult multBuf) {
    Set<PhysicalPartitionKey> keys = multBuf.getAllPhysicalPartitionKeys();
    // creat map to output partId=>PhysicalSources...
    for(PhysicalPartitionKey key: keys) {
      Set<PhysicalSource> set = multBuf.getPhysicalSourcesForPartition(key.getPhysicalPartition());
      LOG.info("for key = " + key + ":");
      for(PhysicalSource ps : set) {
        LOG.info("\t" + ps );
      }
    }
  }
  private void addOneBufferWithKey(MockPhysicalSourceConfigBuilder mpscb, String key)
      throws ParseException, DatabusException {
    ResourceKey rk = new ResourceKey(key);
    PhysicalSourceStaticConfig pssc = mpscb.build(rk);
    _realRelay.addNewBuffer(pssc, _espressoRelayStaticConfig);
  }

  private void removeOneBufferWithKey(MockPhysicalSourceConfigBuilder mpscb, String key)
      throws ParseException, DatabusException {
    ResourceKey rk = new ResourceKey(key);
    PhysicalSourceStaticConfig pssc = mpscb.build(rk);
    _realRelay.removeBuffer(pssc);
  }

  private void validateBufferPartitions(PhysicalPartition pPart, int bufNum) {
    Set<PhysicalSource> pSrcs = _realRelay.getEventBuffer().getPhysicalSourcesForPartition(pPart);
    LOG.info("physical sources for ppart" + pPart + " = " + pSrcs);
    Assert.assertEquals( _realRelay.getEventBuffer().bufsNum(), bufNum, "number of buffers");


  }
  @Test
  public void testAddRemoveBufferBySameResourceKey()
      throws InvalidConfigException, ParseException, DatabusException {
    MockPhysicalSourceConfigBuilder mpscb = new MockPhysicalSourceConfigBuilder();

    int bufNumberPart_1 = 0;
    addOneBufferWithKey(mpscb, "localhost_12932,ucpx,p1_1,OFFLINE");
    bufNumberPart_1 ++;
    addOneBufferWithKey(mpscb, "localhost_12933,ucpx,p1_1,OFFLINE");
    bufNumberPart_1 ++;
    removeOneBufferWithKey(mpscb, "localhost_12932,ucpx,p1_1,OFFLINE");
    bufNumberPart_1 --;
    addOneBufferWithKey(mpscb, "localhost_12932,ucpx,p1_1,SLAVE");
    bufNumberPart_1 ++;
    removeOneBufferWithKey(mpscb, "localhost_12932,ucpx,p1_1,SLAVE");
    bufNumberPart_1 --;
    addOneBufferWithKey(mpscb, "localhost_12932,ucpx,p1_1,MASTER");
    bufNumberPart_1 ++;
    removeOneBufferWithKey(mpscb, "localhost_12932,ucpx,p1_1,MASTER");
    bufNumberPart_1 --;
    addOneBufferWithKey(mpscb, "localhost_12932,ucpx,p1_1,OFFLINE");
    bufNumberPart_1 ++;
    validateBufferPartitions(new PhysicalPartition(1, "ucpx"), bufNumberPart_1 );
    printAllBuffers(_realRelay.getEventBuffer());
  }

  @Test
  public void testAddRemoveBufferByResourceKey()
      throws InvalidConfigException, ParseException, DatabusException {
    String [] rsKeys = new String [] { // should create 2 buffers
        "localhost_12932,ucpx,p1_1,OFFLINE",
        "localhost_12932,ucpx,p1_1,SLAVE",
        "localhost_12932,ucpx,p1_1,MASTER",
        "localhost_12933,ucpx,p1_1,SLAVE",
        "localhost_12932,ucpx,p2_1,OFFLINE"
    };
    MockPhysicalSourceConfigBuilder mpscb = new MockPhysicalSourceConfigBuilder();
    int buffNum = _realRelay.getEventBuffer().bufsNum();

    List<PhysicalSourceStaticConfig> psscList = new ArrayList<PhysicalSourceStaticConfig>();
    for(String rsKey : rsKeys) {
      ResourceKey r = new ResourceKey(rsKey);
      PhysicalSourceStaticConfig pssc = mpscb.build(r);
      psscList.add(pssc);
    }
    // add all buffers
    //List<PhysicalSourceStaticConfig> psscList = mpscb.build();
    for(PhysicalSourceStaticConfig pssc : psscList) {
      _realRelay.addNewBuffer(pssc, _espressoRelayStaticConfig);
    }

    // expected 3 buffers, one default + 2 created from the keys
    int numberOfPhSources = 4; // OFFLINE, SLAVE, MASTER, SLAVE
    Assert.assertEquals( _realRelay.getEventBuffer().bufsNum(), buffNum+2, "number of buffers");
    for(PhysicalSourceStaticConfig pssc : psscList) {
      PhysicalPartition pPart = pssc.getPhysicalPartition();
      Set<PhysicalSource> pSrcs = _realRelay.getEventBuffer().getPhysicalSourcesForPartition(pPart);
      LOG.info("physical sources for ppart" + pPart + " = " + pSrcs);
      if(pPart.getId() == 1) // for partition one we attempted to add buffer 4 times, for 4 different keys/sources
        Assert.assertEquals(pSrcs.size(), numberOfPhSources, "number of mapped ph sources for the part 1");
      else if(pPart.getId() == 2)
        Assert.assertEquals(pSrcs.size(), 1, "number of mapped ph sources for the part 2");
    }
    // remove all buffers
    for(PhysicalSourceStaticConfig pssc : psscList) {
      _realRelay.removeBuffer(pssc);
    }

    // number of referenced physical sources should be 0
    for(PhysicalSourceStaticConfig pssc : psscList) {
      PhysicalPartition pPart = pssc.getPhysicalPartition();
      Set<PhysicalSource> pSrcs = _realRelay.getEventBuffer().getPhysicalSourcesForPartition(pPart);
      LOG.info("physical sources for ppart" + pPart + " = " + pSrcs);
      Assert.assertEquals(pSrcs.size(), 0, "number of mapped ph sources");
    }

    Assert.assertEquals( _realRelay.getEventBuffer().bufsNum(), buffNum+2, "number of buffers after remove");
    _realRelay.getEventBuffer().deallocateRemovedBuffers(true);
    // physically removes all the buffers (except the default one)
    Assert.assertEquals( _realRelay.getEventBuffer().bufsNum(), 1, "number of buffers after deallocate");
  }

  private PhysicalSourceStaticConfig buildPhysicalSourceStaticConfig(short id, String pName, String uri)
      throws com.linkedin.databus.core.util.InvalidConfigException {
    List<LogicalSourceConfig> sources = new ArrayList<LogicalSourceConfig>(2);
    for(short j=0; j<2; j++) {
      LogicalSourceConfig lConfig = new LogicalSourceConfig();
      lConfig.setId(j);
      lConfig.setName("source"+j);
      lConfig.setUri(uri);
      lConfig.setPartitionFunction("whatever");
      lConfig.setPartition(id);
      sources.add(lConfig);
    }
    PhysicalSourceConfig pConfig = new PhysicalSourceConfig(pName+1, uri, id);
    pConfig.setSources(sources);
    return pConfig.build();
  }

  private void addEvents(DbusEventBuffer buf, int num) {
    byte[] schemaId = "abcdefghijklmnop".getBytes();
    int i = 0;
    buf.startEvents();
    for (i = 0; i < num; ++i) {
      DbusEventKey key = new DbusEventKey(RngUtils.randomPositiveLong(0, 1000));
      //short srcId = sources.get((Integer) (RngUtils.randomPositiveShort() % sources.size())).getId().shortValue();
      short srcId = 22;

      int length = 10; // doesn't really matter
      String value = RngUtils.randomString(length); // 1K of row data

      buf.appendEvent(key, (short)10, (short)20, 100, srcId, schemaId,
                                  value.getBytes(), false, null);

    }
    //
    buf.endEvents(i);
  }

  private String printMemory() {
    long mb = 1024*1024;
    Runtime runtime = Runtime.getRuntime();
    //Print used memory
    long total = runtime.totalMemory();
    long free = runtime.freeMemory();
    long used = total - free - _baseMem;
    long max = runtime.maxMemory();
    return "used:" + used/mb + ";free=" + free/mb + ";total=" + total/mb + ";max=" + max/mb;
  }
}