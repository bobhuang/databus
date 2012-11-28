package com.linkedin.databus2.test.integ;

import java.io.IOException;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.TimeoutException;

import org.apache.avro.Schema;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.databus.client.SingleSourceSCN;
import com.linkedin.databus.client.consumer.AbstractDatabusV3Consumer;
import com.linkedin.databus.client.pub.CheckpointPersistenceProvider;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DatabusClientException;
import com.linkedin.databus.client.pub.DatabusV3Registration;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.FetchMaxSCNRequest;
import com.linkedin.databus.client.pub.RegistrationId;
import com.linkedin.databus.client.pub.RelayFindMaxSCNResult;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBufferMult;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.data_model.LogicalSourceId;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.data_model.PhysicalSource;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.NoSuchSchemaException;
import com.linkedin.databus2.test.integ.TestRelayUtils.EspressoRelayTestCluster;
import com.linkedin.databus3.espresso.EspressoRelay;
import com.linkedin.databus3.espresso.client.DatabusHttpV3ClientImpl;

@Test(singleThreaded=true)
public class TestDbusEventBufferMultRemoveBuffer
{
  private static final Logger LOG = Logger.getLogger(TestDbusEventBufferMultRemoveBuffer.class);

  private final static String DB_NAME = "BizProfile"; //because we have to use a real registry - we have to use real DB_NAME
  private final static String DB_SCHEMA = "BizCompany";


  private DbusEventBufferMult _bufMult;
  private DatabusHttpV3ClientImpl _client;


  private Schema _schema;
  private short _srcId = 2;
  private EspressoRelay _realRelay;
  private EspressoRelayTestCluster _testCluster;

  static
  {
    PatternLayout defaultLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c} %m%n");
    ConsoleAppender defaultAppender = new ConsoleAppender(defaultLayout);

    Logger.getRootLogger().removeAllAppenders();
    Logger.getRootLogger().addAppender(defaultAppender);

    Logger.getRootLogger().setLevel(Level.OFF);
    Logger.getRootLogger().setLevel(Level.ERROR);
    Logger.getRootLogger().setLevel(Level.INFO);
    //Logger.getRootLogger().setLevel(Level.DEBUG);
  }

  @BeforeMethod
  public void setUp()
      throws InvalidConfigException, com.linkedin.databus.core.util.InvalidConfigException,
      DatabusException, IOException, DatabusClientException, NoSuchSchemaException, com.linkedin.espresso.common.config.InvalidConfigException {


    _testCluster = new EspressoRelayTestCluster();
    EspressoRelay.StaticConfigBuilder relayCfgBuilder = _testCluster.getEspressoRelayConfigBuilder();
    relayCfgBuilder.getClusterManager().setInstanceName("localhost_11140");
    // add your settings to the config

    // now get the relay with 2 buffers (2 physical partitions)
    _realRelay = _testCluster.createTestEspressoRelay(DB_NAME, 2);
    _bufMult = _realRelay.getEventBuffer();

    _schema = _testCluster.getSchema(DB_NAME, DB_SCHEMA);
    //_pStatConf = TestRelayUtils.getPhysicalSourceStaticConfigs()[0];
    _srcId = _testCluster.getSourceIdForTable(DB_NAME, DB_SCHEMA);
  }

  @AfterMethod
  public void tearDown() {
    _testCluster.shutdown();
  }

  // setup Client
  private void setupClient(String sources) throws IOException, DatabusException, DatabusClientException {
    Assert.assertNotNull(_testCluster);

    // get the default config - update it if needed
    DatabusHttpV3ClientImpl.StaticConfigBuilder configBuilder = _testCluster.getDatabusHttpV3ClientImplStaticConfigBuilder();
    configBuilder.getClusterManager().setInstanceName("localhost_11150");

    // create the client
    _client = _testCluster.createClient(sources);
  }

  // case with bufMult streaming and removing buffers
  // is covered in TestDbusEventBufferMult
  // this case covers Client connectins
  @Test
  public void testAddRemoveBufferWithClient()
      throws IOException, DatabusException, DatabusClientException, TimeoutException, InterruptedException
  {
    // start relay
    _realRelay.start();

    PhysicalSourceStaticConfig pConf1, pConf2;
    // add events
    pConf1 = _testCluster.getPhysicalSourceStaticConfigs()[0];
    pConf2 = _testCluster.getPhysicalSourceStaticConfigs()[1];
    Assert.assertNotNull(pConf1);
    Assert.assertNotNull(pConf2);
    // first buffer
    Assert.assertEquals(6, _testCluster.addSomeEvents(2, 3, _srcId, pConf1.getPhysicalPartition(), _schema));

    // setup client
    // we are interested in two partitions
    String sources = DB_NAME+ "." + DB_SCHEMA + ":" + pConf1.getId();
    sources += "," + DB_NAME+ "." + DB_SCHEMA + ":" + pConf2.getId();

    setupClient(sources);
    LOG.info("subscribing for sources: " + sources);

    // create subscription
    DatabusSubscription ds = createDatabusSubscription(pConf1.getId(), pConf1.getName());
    LOG.info("subscribing for pid=" + pConf1.getId() + ";db=" + pConf2.getName() + ";ds="+ds);

    // subscribe
    DummyV3StreamConsumer consumer1 = new DummyV3StreamConsumer("consumer1");
    DatabusV3Registration reg = _client.registerDatabusListener(consumer1, null, null, ds);
    reg.start();
    LOG.info("creating client: " + _client + "consumer=" + consumer1 + ";ds=" + ds + ";reg=" + reg);

    // create second subscription
    DatabusSubscription ds1 = createDatabusSubscription(pConf2.getId(), pConf2.getName());
    LOG.info("subscribing for pid=" + pConf2.getId() + ";db=" + pConf2.getName() + ";ds="+ds1);

    // let client pick up all the events
    Thread.sleep(1000);
    Assert.assertEquals(consumer1.getEventCounter(), 6);



    // now remove the buffer
    _bufMult.removeBuffer(pConf1);
    _bufMult.deallocateRemovedBuffers(true);

    // should still get events from the other buffer
    _testCluster.addSomeEvents(2, 3, _srcId, pConf2.getPhysicalPartition(), _schema);

    // subscribe
    DummyV3StreamConsumer consumer2 = new DummyV3StreamConsumer("consumer2");
    DatabusV3Registration reg2 = _client.registerDatabusListener(consumer2, null, null, ds1);
    reg2.start();
    LOG.info("creating client2 : " + _client + "consumer=" + consumer2 + ";ds=" + ds1 + ";reg=" + reg);


    Thread.sleep(1000);
    Assert.assertEquals(consumer2.getEventCounter(), 6);
    reg.deregister();
    reg2.deregister();


  }

  // case with bufMult streaming and removing buffers
  // is covered in TestDbusEventBufferMult
  // this case covers Client connectins
  @Test
  public void testAddRemoveBufferWithClient2()
      throws IOException, DatabusException, DatabusClientException, TimeoutException, InterruptedException, Exception
  {
    // start relay
    _realRelay.start();

    PhysicalSourceStaticConfig pConf1;
    // add events
    pConf1 = _testCluster.getPhysicalSourceStaticConfigs()[0];
    Assert.assertNotNull(pConf1);
    // first buffer
    Assert.assertEquals(6, _testCluster.addSomeEvents(2, 3, _srcId, pConf1.getPhysicalPartition(), _schema));

    // setup client
    // we are interested in two partitions
    String sources = DB_NAME+ "." + DB_SCHEMA + ":" + pConf1.getId();

    setupClient(sources);
    LOG.info("subscribing for sources: " + sources);

    // create subscription
    DatabusSubscription ds = createDatabusSubscription(pConf1.getId(), pConf1.getName());
    LOG.info("subscribing for pid=" + pConf1.getId() + ";db=" + ";ds="+ds);

    // subscribe
    DummyV3StreamConsumer consumer1 = new DummyV3StreamConsumer("consumer1");
    DatabusV3Registration reg = _client.registerDatabusListener(consumer1, null, null, ds);
    
    // set the starting SCN to fetch event from databus
    long startingSCN = 1;
    setCheckpoint(startingSCN, reg, ds, DB_NAME, pConf1.getPhysicalPartition().getId());

    // Check if the starting SCN exists in relay or not
    // If not, we need to bootstrap
    //  long minSCN = getMinSCN(reg);
 
    setCheckpoint(2, reg, ds, DB_NAME, pConf1.getPhysicalPartition().getId());

    // Start the replication stream to DbusConsumer
    reg.start();
    LOG.info("creating client: " + _client + "consumer=" + consumer1 + ";ds=" + ds + ";reg=" + reg);

    Thread.sleep(1000);
    Assert.assertEquals(consumer1.getEventCounter(), 6);
    reg.deregister();
  }

  private void setCheckpoint(long scn, DatabusV3Registration reg, DatabusSubscription ds, String dbName, int partitionId)
  throws Exception
  {
      CheckpointPersistenceProvider ckp = reg.getCheckpoint();
      Checkpoint cp = new Checkpoint();
      cp.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
      cp.setWindowScn(scn);
      cp.setWindowOffset(-1);
      RegistrationId regId = reg.getId();
      List<DatabusSubscription> dsList = new Vector<DatabusSubscription>();
      dsList.add(ds);
      ckp.storeCheckpointV3(dsList, cp, regId);
      LOG.info("set checkpoint at scn " + cp.getWindowScn());
      return;
  }

  private long getMinSCN(DatabusV3Registration reg) throws Exception
  {
     FetchMaxSCNRequest scnReq = new FetchMaxSCNRequest(500, 1);
     RelayFindMaxSCNResult resultFind = reg.fetchMaxSCN(scnReq);
     long eSCN = -1;
     if (resultFind!=null)
     {
        if((resultFind.getResultSummary() == RelayFindMaxSCNResult.SummaryCode.SUCCESS) ||
           (resultFind.getResultSummary() == RelayFindMaxSCNResult.SummaryCode.PARTIAL_SUCCESS))
            {
               eSCN = ((SingleSourceSCN)resultFind.getMinSCN()).getSequence();
            } else /* 
            //comment out because we shouldn't see empty external view, 
            //if we did, eSCN will be null, and NPE will be thrown
            if (resultFind.getResultSummary() == RelayFindMaxSCNResult.SummaryCode.EMPTY_EXTERNAL_VIEW) 
            {
               eSCN = new EspressoSCN(EspressoSCN.START_GENERATION, EspressoSCN.START_SEQUENCE);
            } else */
            {
               // noop eSCN=null
            }
     }
     else
     {
         throw new Exception("resultFind is null");
     }
     return eSCN;
  }

  public static DatabusSubscription createDatabusSubscription(int partId, String dbName) {
    /**
     * Build the DatabusSubscription object based on information from properties / ClusterManager
     */
    PhysicalSource ps = PhysicalSource.ANY_PHISYCAL_SOURCE;
    LogicalSourceId.Builder lidB = new LogicalSourceId.Builder();
    lidB.setId((short)partId);
    LogicalSourceId lid = lidB.build();
    PhysicalPartition pp = new PhysicalPartition(partId, dbName);
    DatabusSubscription ds = new DatabusSubscription(ps, pp, lid);
    return ds;
  }


  class DummyV3StreamConsumer extends AbstractDatabusV3Consumer
  {
    private final String _name;
    private int _eventCounter;

    public DummyV3StreamConsumer(String name) {
      super();
      _name = name;
      _eventCounter = 0;
    }

    @Override
    public ConsumerCallbackResult onStartConsumption() {
      return ConsumerCallbackResult.SUCCESS;
    }
    public int getEventCounter() {
      return _eventCounter;
    }

    @Override
    public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder) {
      _eventCounter++;
      LOG.info("CONSUMER " + _name + " got event. counter=" + _eventCounter);

      return ConsumerCallbackResult.SUCCESS;
    }
  }
}


