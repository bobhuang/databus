package com.linkedin.databus3.rpl_dbus_manager;

import java.net.UnknownHostException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.easymock.EasyMock;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus3.cm_utils.ClusterManagerRelayCoordinates;
import com.linkedin.databus3.cm_utils.ClusterManagerResourceKey;
import com.linkedin.databus3.cm_utils.ClusterManagerStaticConfigBaseBuilder;
import com.linkedin.databus3.cm_utils.ClusterManagerUtilsException;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.NotificationContext.Type;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.model.ExternalView;
import com.mysql.jdbc.Connection;

public class TestRplDbusManager
{

  public static final String MODULE = TestRplDbusMonitor.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  Map<ClusterManagerRelayCoordinates, List<ClusterManagerResourceKey>> _externalView =
      new HashMap<ClusterManagerRelayCoordinates, List<ClusterManagerResourceKey>>();

  private RplDbusManagerTest _rdm;

  private RplDbusNodeCoordinates _rplDbusRelay;
  private RplDbusMysqlCoordinates _rplDbusMysql;

  RplDbusCMAdapterTest _cmAdapter;

  static
  {
    //PatternLayout defaultLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c} %m%n");
    PatternLayout defaultLayout = new PatternLayout("%m%n");
    ConsoleAppender defaultAppender = new ConsoleAppender(defaultLayout);

    Logger.getRootLogger().removeAllAppenders();
    Logger.getRootLogger().addAppender(defaultAppender);

    Logger.getRootLogger().setLevel(Level.OFF);
    Logger.getRootLogger().setLevel(Level.ERROR);


    //Logger l = Logger.getLogger(RplDbusManager.class.getName());
    //l.setLevel(Level.DEBUG);
    //Logger.getRootLogger().setLevel(Level.INFO);
    Logger.getRootLogger().setLevel(Level.DEBUG);
  }

  private final String _testRelayHost = "rhost";
  // External view simulator
  private static String _ev0 = "{" +
      "  \"id\" : \"LOCALHOSTDB\"," +
      "  \"mapFields\" : {" +
      "    \"snhost_12818,BizProfile,p0_1,SLAVE\" : {" +
      "      \"rhost_11130\" : \"ONLINE\"" + "," +
      "      \"rhost_11140\" : \"ONLINE\"" +
      "    }," +
      "    \"snhost_12918,BizProfile,p7_1,SLAVE\" : {" +
      "      \"rhost_11130\" : \"ONLINE\"" +  "," +
      "      \"rhost_11140\" : \"ONLINE\"" +
      "    }" +
      "  }," +
      "  \"simpleFields\" : {" +
      "  }," +
      "  \"deltaList\" : [ ]," +
      "  \"listFields\" : {" +
      "  }" +
      "}" ;
  private static String _ev1 = "{" +
      "  \"id\" : \"LOCALHOSTDB\"," +
      "  \"mapFields\" : {" +
      "    \"snhost_12818,BizProfile,p0_1,SLAVE\" : {" +
      "      \"rhost_11130\" : \"OFFLINE\"" + "," +
      "      \"rhost_11140\" : \"OFFLINE\"" +
      "    }," +
      "    \"snhost_12918,BizProfile,p7_1,SLAVE\" : {" +
      "      \"rhost_11130\" : \"ONLINE\"" +  "," +
      "      \"rhost_11140\" : \"ONLINE\"" +
      "    }" +
      "  }," +
      "  \"simpleFields\" : {" +
      "  }," +
      "  \"deltaList\" : [ ]," +
      "  \"listFields\" : {" +
      "  }" +
      "}" ;

  private static String _ev2 = "{" +
      "  \"id\" : \"LOCALHOSTDB\"," +
      "  \"mapFields\" : {" +
      "    \"snhost_12918,BizProfile,p0_1,SLAVE\" : {" +
      "      \"rhost_11130\" : \"ONLINE\"" + "," +
      "      \"rhost_11140\" : \"ONLINE\"" +
      "    }," +
      "    \"snhost_12918,BizProfile,p7_1,SLAVE\" : {" +
      "      \"rhost_11130\" : \"ONLINE\"" +  "," +
      "      \"rhost_11140\" : \"ONLINE\"" +
      "    }" +
      "  }," +
      "  \"simpleFields\" : {" +
      "  }," +
      "  \"deltaList\" : [ ]," +
      "  \"listFields\" : {" +
      "  }" +
      "}" ;

  private static String _ev3 = "{" +
      "  \"id\" : \"LOCALHOSTDB\"," +
      "  \"mapFields\" : {" +
      "    \"snhost_12818,BizProfile,p0_1,SLAVE\" : {" +
      "      \"rhost_11140\" : \"ONLINE\"" +
      "    }," +
      "    \"snhost_12818,BizProfile,p1_1,MASTER\" : {" +
      "      \"rhost_11140\" : \"ONLINE\"" +
      "    }," +
      "    \"snhost_12918,BizProfile,p7_1,SLAVE\" : {" +
      "      \"rhost_11130\" : \"ONLINE\"" +  "," +
      "      \"rhost_11140\" : \"ONLINE\"" +
      "    }" +
      "  }," +
      "  \"simpleFields\" : {" +
      "  }," +
      "  \"deltaList\" : [ ]," +
      "  \"listFields\" : {" +
      "  }" +
      "}" ;

  private static String _ev4 = "{" +
      "  \"id\" : \"LOCALHOSTDB\"," +
      "  \"mapFields\" : {" +
      "    \"snhost_12818,BizProfile,p0_1,SLAVE\" : {" +
      "      \"rhost_11130\" : \"ONLINE\"" +
      "    }," +
      "    \"snhost_12818,BizProfile,p1_1,MASTER\" : {" +
      "      \"rhost_11130\" : \"ONLINE\"" +
      "    }," +
      "    \"snhost_12918,BizProfile,p7_1,SLAVE\" : {" +
      "      \"rhost_11130\" : \"OFFLINE\"" +  "," +
      "      \"rhost_11140\" : \"ONLINE\"" +
      "    }" +
      "  }," +
      "  \"simpleFields\" : {" +
      "  }," +
      "  \"deltaList\" : [ ]," +
      "  \"listFields\" : {" +
      "  }" +
      "}" ;

  private static String _real_ev5 = "{" +
      "  \"id\" : \"LOCALHOSTDB\"," +
      "  \"mapFields\" : {" +
      "    \"eat1-app13.corp.linkedin.com_12818,BizProfile,p0_1,SLAVE\" : {" +
      "      \"localhost_11140\" : \"ONLINE\"" +
      "    }," +
      "    \"snhost_12818,BizProfile,p1_1,MASTER\" : {" +
      "      \"rhost_11140\" : \"ONLINE\"" +
      "    }," +
      "    \"snhost_12918,BizProfile,p7_1,SLAVE\" : {" +
      "      \"rhost_11130\" : \"ONLINE\"" +  "," +
      "      \"rhost_11140\" : \"ONLINE\"" +
      "    }" +
      "  }," +
      "  \"simpleFields\" : {" +
      "  }," +
      "  \"deltaList\" : [ ]," +
      "  \"listFields\" : {" +
      "  }" +
      "}" ;


  private static String [] _testEVs = new String [] {
    _ev0, _ev1, _ev2, _ev3, _ev4, _real_ev5
  };
  public static ExternalView getExternalView(int evNum) {
    String ev = _testEVs[evNum];
    ZNRecordSerializer zs = new ZNRecordSerializer();
    ZNRecord zr = (ZNRecord) zs.deserialize(ev.getBytes());
    return new ExternalView(zr);
  }


  // setup rpldbus adapters, rpldbuscmadapter and rpldbusmanager
  @BeforeMethod
  public void buildTestSet() throws ParseException, RplDbusException, ClusterManagerUtilsException, InvalidConfigException {
    // mapping from storage node  to storage node mysql port
    Map<String, Integer> mysqlPortMap = new HashMap<String, Integer>(2);
    mysqlPortMap.put("snhost_12818", 12818 + 1);
    mysqlPortMap.put("snhost_12918", 12918 + 1);

    _cmAdapter = new RplDbusCMAdapterTest("", "");
    _cmAdapter.setClusterConfig(mysqlPortMap); // set the mapping


    int relayPort = 11130;
    int rplDbusMysqlPort = 11131;
    String relayCoordinates = _testRelayHost+"_"+relayPort;
    String mysqlCoordiantes = _testRelayHost+"_"+rplDbusMysqlPort;
    RplDbusManagerConfigBuilder configBuilder = new RplDbusManagerConfigBuilder();
    configBuilder.setEnabled(true);
    configBuilder.setInstanceName(relayCoordinates);
    configBuilder.setRelayRplDbusMapping(relayCoordinates + ":" + mysqlCoordiantes);
    RplDbusManagerStaticConfig config = configBuilder.build();


    // create RplDbus adapters
    // for rpldbus on relay 11130
    _rplDbusRelay = new RplDbusNodeCoordinates(_testRelayHost, relayPort);
    _rplDbusMysql = new RplDbusMysqlCoordinates(_testRelayHost, rplDbusMysqlPort);

    // create rpldbusmanager
    _rdm = new RplDbusManagerTest(config);
    _rdm.setCMAdapter(_cmAdapter);

    _rdm.start();
    sleepSec(1);
  }


  // test connection to the real CM (not a unit test)
  // @Test
  public void testRplDbusCMAdapter() throws ClusterManagerUtilsException {
    RplDbusCMAdapter _rdbCMAdapter = new RplDbusCMAdapter("ecluster", "localhost:21810");
    sleepSec(1);
    _rdbCMAdapter.connect();

    // get config
    String key = "localhost_12818";
    String port = _rdbCMAdapter.getClusterConfig(key);
    Assert.assertEquals(port, "14100");
    key = "localhost_12918";
    port = _rdbCMAdapter.getClusterConfig(key);
    Assert.assertEquals(port, "3306");

    _rdbCMAdapter.disconnect();
  }

  // test works over real mysql
  @Test
  public void testValidateTask() throws InvalidConfigException, RplDbusException, ClusterManagerUtilsException {
    RplDbusManagerConfigBuilder configBuilder = new RplDbusManagerConfigBuilder();
    configBuilder.setEnabled(true);
    configBuilder.setInstanceName("localhost_11140");
    configBuilder.setMysqlPortMapping("eat1-app13.corp.linkedin.com_12818:eat1-app13.corp.linkedin.com_3306");
    configBuilder.setRelayRplDbusMapping("localhost_11140:localhost_22000:");
    configBuilder.setRplDbusMysqlUser("root");
    configBuilder.setRplDbusMysqlPassword("");

    _rdm = new RplDbusManagerTest(configBuilder.build());
    _rdm.setCMAdapter(_cmAdapter);
    //RplDbusAdapter adapter = new RplDbusAdapter(configBuilder.build(), new RplDbusMysqlCoordinatesWithCreds("localhost", 29000, "root", ""));
    //_rdm.setRplDbusAdapter(new RplDbusNodeCoordinates("localhost", 11140), adapter);
    _rdm.start();

    RplDbusManager.UpdateTask ut = _rdm.new UpdateTask();
    updateWithExternalView(5);
    _rdm.updateRecordsInternal(_externalView);
    ut.run();
  }



  // do not run as a part of hudson. run manually - make sure you have "ecluster" running on localhost
  //@Test
  public void testRpldDbusManagerAgainstRealCM()
      throws RplDbusException, ClusterManagerUtilsException, InvalidConfigException {
    String zkConnectString = "localhost:21810";

    RplDbusManagerConfigBuilder configBuilder = new RplDbusManagerConfigBuilder();
    configBuilder.setEnabled(true);
    configBuilder.setRelayClusterName("ecluster");
    configBuilder.setRelayZkConnectString(zkConnectString);

    // TODO - create class to take care of this parsing
    String relay = "localhost" + RplDbusNodeCoordinates.HOSTNMAME_PORT_SEPARATOR + "11140";
    String rplDbus = "localhost"  + RplDbusNodeCoordinates.HOSTNMAME_PORT_SEPARATOR + "29000";
    configBuilder.setRelayRplDbusMapping(relay + ":" + rplDbus);

    configBuilder.setRplDbusMysqlUser("root");
    RplDbusManagerStaticConfig config = configBuilder.build();

    RplDbusManager rdm = new RplDbusManager(config);
    RplDbusCMAdapter cmAdapter = new RplDbusCMAdapter(config.getRelayClusterName(), config.getRelayZkConnectString());
    rdm.setCMAdapter(cmAdapter);
    rdm.start();
    sleepSec(1);
    LOG.info("rdm = " + rdm.toString());
  }



  @Test
  public void testRpldDbusManagerSingle() throws ClusterManagerUtilsException, InvalidConfigException {
    testRpldDbusManagerSingle(true);
    //testRpldDbusManagerSingle(false); //not really used
  }

  private void testRpldDbusManagerSingle(boolean useNotify) throws ClusterManagerUtilsException, InvalidConfigException {
    LOG.info("running testRpldDbusManagerSingle with useNotify=" + useNotify);
    Assert.assertNotNull(_rdm);

    // this test sets RDManager to control one relay only and ignore others
    //String thisRelay = _testRelayHost + RplDbusNodeCoordinates.HOSTNMAME_PORT_SEPARATOR + "11130";
    //_rdm.setThisRelay(thisRelay);
    //_rdm.setRplDbusAdapter(new RplDbusNodeCoordinates(thisRelay), null); // no real mysql
    _rdm.skipValidation = true;

    // initial update, mapps to 12919 and 12819
    updateWithExternalView(0);
    LOG.info("about to update with this testView: \n" + RplDbusCMAdapter.buildString(_externalView));
    updateRecords(useNotify);
    LOG.info("rdm = \n" + _rdm.toString());
    // validate the mapping (rpldbus port to snMysql ports)
    validateSet(11131, new int [] {12919, 12819});

    // the other relay should be ignored
    //RplDbusMysqlCoordinates rplDbusMysql = new RplDbusMysqlCoordinates(_testSNHost, 11141);
    //Assert.assertNull(_rdm.getSNMysqlForRplDbusMysql(rplDbusMysql));

    // partition on 12818 goes OFFLINE
    updateWithExternalView(1);
    LOG.info("about to update2 with this testView: \n" + RplDbusCMAdapter.buildString(_externalView));
    updateRecords(useNotify);
    LOG.info("rdm = \n" + _rdm.toString());
    // now relay on 11130(11131) should map to only one SN (12919)
    validateSet(11131, new int [] {12919});

    // new update - move all the partitions from 12818 to 12918
    updateWithExternalView(2);
    LOG.info("about to update3 with this testView: \n" + RplDbusCMAdapter.buildString(_externalView));
    //_rdm.updateRecords(_externalView);
    updateRecords(useNotify);
    LOG.info("rdm = \n" + _rdm.toString());
    // new port
    validateSet(11131, new int [] {12919});

    // now move all the partitions from 12918 to 12818
    updateWithExternalView(4);
    LOG.info("about to update4 with this testView: \n" + RplDbusCMAdapter.buildString(_externalView));
    //_rdm.updateRecords(_externalView);
    updateRecords(useNotify);
    LOG.info("rdm = \n" + _rdm.toString());
    // new port
    validateSet(11131, new int [] {12819});

  }

  private void updateRecords(boolean useNotify) {
    if(useNotify) {
      _rdm.notifyUpdate();
      try {
        Thread.sleep(700);
      } catch (InterruptedException e)  { }
    } else {
      _rdm.updateRecords(_externalView);
    }
  }

  // simulate external view update
  private void updateWithExternalView(int num) throws ClusterManagerUtilsException {
    ExternalView ev = getExternalView(num);
    List<ExternalView> evList = new ArrayList<ExternalView>(2);
    evList.add(ev);
    NotificationContext nc = new NotificationContext(null);
    nc.setType(Type.INIT);

    _cmAdapter.onExternalViewChange(evList, nc);
    _externalView = _cmAdapter.getCurrentExternalViews();
  }

  // make sure relay with port 'relayPort' has storage nodes with ports in 'snodesPorts'
  private void validateSet(int relayPort, int [] snodesPorts) {
    Arrays.sort(snodesPorts);

  //  RplDbusMysqlCoordinates rplDbusMysql = new RplDbusMysqlCoordinates(_testRelayHost, relayPort);
    Set<RplDbusMysqlCoordinates> snodes = _rdm._mySnViewMysql.keySet();
    Assert.assertNotNull(snodes);
    Assert.assertEquals(snodes.size(), snodesPorts.length);

    for(RplDbusNodeCoordinates snode : snodes) {
      int find = Arrays.binarySearch(snodesPorts, snode.getPort());
      Assert.assertTrue(find > -1);
    }
  }

  public static void sleepSec(int time) {
    try
    {
      Thread.sleep(time * 1000);
    }
    catch (InterruptedException e)  { }
  }

  @Test
  public void testRplDbusMySqlInstance() throws InvalidConfigException, RplDbusException, ClusterManagerUtilsException {

    RplDbusManagerConfigBuilder confBuilder = new RplDbusManagerConfigBuilder();
    String firstSnString = "localhost_12818";
    String firstSnMySqlString = "localhost_3306";
    String secondSnString = "localhost_12918";
    String secondSnMySqlString = "localhost_14108";
    confBuilder.setMysqlPortMapping(firstSnString+":" + firstSnMySqlString + ","
                                   + secondSnString + ":" + secondSnMySqlString);

    RplDbusMySqlInstanceRegistry mySqlInst = new RplDbusMySqlInstanceRegistry(confBuilder.build(), null);

    Assert.assertNotNull(mySqlInst);
    RplDbusNodeCoordinates firstMySql = new RplDbusNodeCoordinates(firstSnMySqlString);
    RplDbusNodeCoordinates firstSn = new RplDbusNodeCoordinates(firstSnString);
    Assert.assertEquals(mySqlInst.getMysqlInstance(firstSn),firstMySql);

    RplDbusNodeCoordinates secondMySql = new RplDbusNodeCoordinates(secondSnMySqlString);
    RplDbusNodeCoordinates secondSn = new RplDbusNodeCoordinates(secondSnString);

    Assert.assertEquals(mySqlInst.getMysqlInstance(secondSn),secondMySql);
    try {
      mySqlInst.getMysqlInstance(new RplDbusNodeCoordinates("nonexistinhost_3333"));
      Assert.fail("should fail for nonexistant host");
    } catch (Exception e) {/*expected*/}
  }

  @Test
  public void testRplDbusMySqlInstanceAgainstCM()
      throws ClusterManagerUtilsException, InvalidConfigException, RplDbusException {

    Map<String, Integer> mysqlPortMap = new HashMap<String, Integer>(2);
    mysqlPortMap.put(RplDbusCMAdapterTest.SN1, RplDbusCMAdapterTest.SN1_MYSQL_PORT);
    mysqlPortMap.put(RplDbusCMAdapterTest.SN2, RplDbusCMAdapterTest.SN2_MYSQL_PORT);


    // test CM based mapping
    RplDbusCMAdapterTest cmAdapter = new RplDbusCMAdapterTest("", "");
    cmAdapter.setClusterConfig(mysqlPortMap);
    RplDbusMySqlInstanceRegistry mySqlInst = new RplDbusMySqlInstanceRegistry(new RplDbusManagerConfigBuilder().build(), cmAdapter);
    Assert.assertNotNull(mySqlInst);
    RplDbusNodeCoordinates firstSn = new RplDbusNodeCoordinates(RplDbusCMAdapterTest.SN1);
    RplDbusMysqlCoordinates firstSnMysql = new RplDbusMysqlCoordinates(firstSn.getHostname(), RplDbusCMAdapterTest.SN1_MYSQL_PORT);
    Assert.assertEquals(mySqlInst.getMysqlInstance(firstSn),firstSnMysql);

    RplDbusNodeCoordinates secondSn = new RplDbusNodeCoordinates(RplDbusCMAdapterTest.SN2);
    RplDbusMysqlCoordinates secondMySql = new RplDbusMysqlCoordinates(secondSn.getHostname(), RplDbusCMAdapterTest.SN2_MYSQL_PORT);
    Assert.assertEquals(mySqlInst.getMysqlInstance(secondSn),secondMySql);

    try {
      mySqlInst.getMysqlInstance(new RplDbusNodeCoordinates("nonexistinhost_3333"));
      Assert.fail("should fail");
    } catch (Exception e) {/*expected*/}

  }


  @Test
  public void testCreateThreadName() {
    RplDbusNodeCoordinates node = new RplDbusNodeCoordinates("somehost", 100);
    Assert.assertEquals(RplDbusAdapter.createThreadNameForNode(node), "somehost:100");
    node = new RplDbusNodeCoordinates("somehost.domain.com", 100);
    System.out.println("CREATE" + RplDbusAdapter.createThreadNameForNode(node));
    Assert.assertEquals(RplDbusAdapter.createThreadNameForNode(node), "somehost_domain_com:100");
  }

  @Test
  public void testRplDbusManagerConfigBuilder() throws UnknownHostException, InvalidConfigException {
    RplDbusManagerConfigBuilder bld = new RplDbusManagerConfigBuilder();
    bld.setInstanceName("somehost_port");
    RplDbusManagerStaticConfig config = bld.build();
    Assert.assertEquals(config.getInstanceName(), "somehost_port");

    bld.setInstanceName(ClusterManagerStaticConfigBaseBuilder.USE_LOCAL_HOST_NAME + "_port");
    config = bld.build();
    String hostname = java.net.InetAddress.getLocalHost().getCanonicalHostName();
    Assert.assertEquals(config.getInstanceName(), hostname + "_port");

    bld.setRelayRplDbusMapping("myhost_123:myhost_125");
    config = bld.build();
    Assert.assertEquals(config.getRelayRplDbusMapping(), "myhost_123:myhost_125");

    bld.setRelayRplDbusMapping("useLocalHostName_123:useLocalHostName_125");
    config = bld.build();
    Assert.assertEquals(config.getRelayRplDbusMapping(), hostname + "_123:" + hostname + "_125");
  }

  @Test
  public void testSlaveCaughtUp() throws RplDbusException, InvalidConfigException {

    RplDbusManagerConfigBuilder configBuilder = new RplDbusManagerConfigBuilder();
    configBuilder.setEnabled(true);
    int port = 2020;
    String hostname = RplDbusNodeCoordinates.convertLocalHostToFqdn("localhost");
    configBuilder.setRelayRplDbusMapping(hostname + "_" + port + ":" + hostname + "_" + (port +1));
    configBuilder.setInstanceName(hostname + "_" + port);
    RplDbusManagerStaticConfig config = configBuilder.build();


    RplDbusMysqlCoordinatesWithCreds node = new RplDbusMysqlCoordinatesWithCreds(_rplDbusMysql.getHostname(),
                                                                                 _rplDbusMysql.getPort());

    RplDbusAdapterTest adapter = new RplDbusAdapterTest(config, node);
    RplDbusManagerTest.setAdapter(adapter); // create mock adapter
    RplDbusManagerTest rdm = new RplDbusManagerTest(config);
    rdm._useRealSlaveThreadCaughtUp = true;


    /// create fake master adapter
    RplDbusMasterAdapterTest masterAdapter = new RplDbusMasterAdapterTest(node);
    RplDbusMasterStateTest masterState = new RplDbusMasterStateTest();
    masterAdapter.addState(masterState);

    // add fake master adapter to the fake rpldbus adapter
    //adapter.setRpldDbusMasterAdapter(masterAdapter);
    adapter.setRpldDbusMasterAdapter(masterAdapter);

    RplDbusStateTest state = new RplDbusStateTest(true,  node);
    adapter.addState(node,  state);

    //test few scenarios
    // all happy
    state.setExecMasterLogPos(123);
    state.setMasterLogFile("file.001");
    state.setReadMasterLogPos(123);
    state.setRelayMasterLogFile("file.001");

    masterState.setFile("file.001");
    masterState.setPosition(123);

    Assert.assertTrue(rdm.slaveThreadCaughtUp(node));

    // slave behind master , but in the same file
    state.setReadMasterLogPos(122);
    Assert.assertFalse(rdm.slaveThreadCaughtUp(node));
    state.setReadMasterLogPos(123);


    // slave behind master, different file
    state.setRelayMasterLogFile("file.000");
    Assert.assertFalse(rdm.slaveThreadCaughtUp(node));
    state.setRelayMasterLogFile("file.001");

    // slave SQL THREAD behind IO THREAD
    state.setExecMasterLogPos(124);
    Assert.assertFalse(rdm.slaveThreadCaughtUp(node));
    state.setExecMasterLogPos(123);
    state.setMasterLogFile("file.002");
    Assert.assertFalse(rdm.slaveThreadCaughtUp(node));
    state.setMasterLogFile("file.001");

    // simulate situation when master not available
    masterAdapter.throwExceptionWhenGettingState(true);
    masterState.setFile("file.003");; // even though master pos is smaller we should ignore it, bacause master is 'unavailable'
    Assert.assertTrue(rdm.slaveThreadCaughtUp(node));
    masterAdapter.throwExceptionWhenGettingState(false);
  }

  /*
   * verifies that we send a correct update to the relay cluster manager , based on our config
   * at a time of initialization.
   * NOTE, NOTE A UNIT TEST, only runs if there is a real mysql on this port
   */
  //@Test
  void testUpdateRelayClusterManagerWithRelay2RplMapping()
      throws InvalidConfigException, RplDbusException, ClusterManagerUtilsException {

    RplDbusManagerConfigBuilder configBuilder = new RplDbusManagerConfigBuilder();
    String mapping = "localhost_11140:localhost_29000";
    Map<String, String> expectedMapping = new HashMap<String, String>();
    expectedMapping.put("localhost_11140", "29000");
    //expectedMapping.put("localhost_11141", "29001");
    configBuilder.setRelayRplDbusMapping(mapping);
    configBuilder.setInstanceName("localhost_11140");
    configBuilder.setEnabled(true);

    RplDbusManager rdm = new RplDbusManager(configBuilder.build());
    RplDbusCMAdapterTest cmAdapter = new RplDbusCMAdapterTest(null, null);
    rdm.setCMAdapter(cmAdapter);

    rdm.start();
    sleepSec(1);

    // verify the mapping
    Assert.assertTrue(cmAdapter.isSameRelayConfigs(expectedMapping));
  }
  ////////////////////////////////////////////////////////////////////////////////////////////////
  //---------------------------------------
  // mock classes
  //---------------------------------------------
  ////////////////////////////////////////////////////////////////////////////////////////////////
  private static class RplDbusManagerTest extends RplDbusManager {
    public boolean skipValidation = false;
    public boolean _useRealSlaveThreadCaughtUp = false;
    public static RplDbusAdapter _adapter;

    public static void setAdapter(RplDbusAdapter adapter) {
      _adapter = adapter;
    }


    public RplDbusManagerTest(RplDbusManagerStaticConfig config) throws RplDbusException,
        InvalidConfigException
    {
      super(config);
    }
    @Override
    void validateCurrentState() throws RplDbusException {
      if(!skipValidation)
        super.validateCurrentState();
    }

    @Override
    protected RplDbusAdapter createRplDbusAdapter(RplDbusManagerStaticConfig config, RplDbusMysqlCoordinatesWithCreds rplDbusWCred)
        throws RplDbusException {
      if(_adapter == null) // allows one to put a mock adapter.
        _adapter = new RplDbusAdapterTest(config, rplDbusWCred);
      return _adapter;
    }

    @Override
    public boolean slaveThreadCaughtUp(RplDbusMysqlCoordinates snMysqlMasterToAdd) throws RplDbusException {
      if(_useRealSlaveThreadCaughtUp)
        return super.slaveThreadCaughtUp(snMysqlMasterToAdd);

      return true;
    }
  }
  public static class RplDbusCMAdapterTest extends RplDbusCMAdapter {

    public static String SN1 = "locahost_12345";
    public static String SN2 = "locahost_12346";
    public static int SN1_MYSQL_PORT = 12355;
    public static int SN2_MYSQL_PORT = 12356;

    private Map<String, Integer> _mysqlPortMap;

    private Map<String, String> _relayConfigs;

    public RplDbusCMAdapterTest(String clusterName, String zkConnectString) throws ClusterManagerUtilsException
    {
      super(clusterName, zkConnectString);
    }

    @Override
    public void writeRelayConfigs(Map<String, String> relayConfigs) {
      _relayConfigs = relayConfigs;
    }
    public boolean isSameRelayConfigs(Map<String, String> relayConfigs) {
      return relayConfigs.equals(_relayConfigs);
    }

    @Override
    public void setListener(RplDbusCMAdapterListener listener) throws ClusterManagerUtilsException {
      return;
    }

    @Override
    public String getClusterConfig(String key) {
      return _mysqlPortMap.get(key).toString();
    }
    // set the mapping for conig
    public void setClusterConfig(Map<String, Integer> map) {
      _mysqlPortMap = map;
    }
  }

  //
  //  fake RpldDbusMasterAdapterTest
  //
  //
  private static class RplDbusMasterAdapterTest extends RplDbusMasterAdapter {
    public RplDbusMasterAdapterTest(RplDbusMysqlCoordinatesWithCreds rNode) throws RplDbusException
    {
      super(rNode);
    }

    RplDbusMasterStateTest _state;
    boolean _throwExceptionOnGetState = false;

    public void addState(RplDbusMasterStateTest state) {
      _state = state;
    }
    @Override
    public RplDbusMasterState getMasterState() throws RplDbusException {
      if(_throwExceptionOnGetState)
        throw new RplDbusException("kind of cannot connect");
      if(_state == null)
        _state = new RplDbusMasterStateTest();

      _state.setServerId(112233);

      return _state;
    }

    public void throwExceptionWhenGettingState(boolean t) {
      _throwExceptionOnGetState = t;
    }
  }

  /**
   * fake class for rpldbusAdapter
   *
   */
  private static class RplDbusAdapterTest extends RplDbusAdapter {
    private final Map<String, RplDbusState> _stateMap;
    private RplDbusMasterAdapterTest _rplDbusMasterAdapterTest;
    private boolean _useRealStates = false;

    public RplDbusAdapterTest(RplDbusManagerStaticConfig config,
                              RplDbusMysqlCoordinatesWithCreds rNode) throws RplDbusException  {
      super(config, rNode);
      _stateMap = new HashMap<String, RplDbusState>();
    }

    @Override
    public RplDbusMasterAdapter createRplDbusMasterAdapter(RplDbusMysqlCoordinates node) throws RplDbusException {
      RplDbusMysqlCoordinatesWithCreds nodeWC = new RplDbusMysqlCoordinatesWithCreds(node.getHostname(),
                                                                                   node.getPort(),
                                                                                   "", "");
      if(_rplDbusMasterAdapterTest == null)
        _rplDbusMasterAdapterTest = new RplDbusMasterAdapterTest(nodeWC);

      return _rplDbusMasterAdapterTest;
    }

    public void setRpldDbusMasterAdapter(RplDbusMasterAdapterTest masterAdapter) {
      _rplDbusMasterAdapterTest = masterAdapter;
    }
    @Override
    public void connect() throws RplDbusException {
      return;
    }
    // for test
    public void addState(RplDbusMysqlCoordinates storageNode, RplDbusStateTest state) {
      _stateMap.put(RplDbusAdapter.createThreadNameForNode(storageNode), state);
    }

    @Override
    public void stopAllThreads(boolean reset) throws RplDbusException {
      return; // TBD - add mocks here
    }

    @Override
    public void connectToNode(RplDbusMysqlCoordinates storageNode, long offset) throws RplDbusException {
      _stateMap.put(RplDbusAdapter.createThreadNameForNode(storageNode), new RplDbusStateTest(true, storageNode));

      RplDbusMysqlCoordinatesWithCreds storageNodeWCreds = new RplDbusMysqlCoordinatesWithCreds(
                                storageNode.getHostname(), storageNode.getPort(), "rplespresso", "espresso");

      Connection con = null;
      try
      {
      // create some mocks
        PreparedStatement st = EasyMock.createMock(PreparedStatement.class);
        EasyMock.expect(st.executeQuery()).andReturn(null).anyTimes();
        st.close();
        EasyMock.expectLastCall().anyTimes(); // for calls return void this is the format
        EasyMock.replay(st);

        con = EasyMock.createMock(Connection.class);
        // here is the sequence of call as we expect is - see the LOG statements
        LOG.info("Expecting for storage: " + storageNode + "; cmd = " + RplDbusAdapter.RPL_DBUS_STOP_SLAVE_STMT);
        EasyMock.expect(con.prepareStatement(RplDbusAdapter.RPL_DBUS_STOP_SLAVE_STMT)).andReturn(st).times(1);

        String setGlobalCommand = String.format(RplDbusAdapter.RPL_DBUS_SET_SLAVE_NAME_STMT,
                                                RplDbusAdapter.createThreadNameForNode(storageNode));
        LOG.info("Expecting for storage: " + storageNode + "; cmd = " + setGlobalCommand);
        EasyMock.expect(con.prepareStatement(setGlobalCommand)).andReturn(st).times(1);

        String changeMasterCommand = RplDbusAdapter.prepareChangeMasterStatment(storageNodeWCreds, offset);
        String changeMasterCommandWithCred = String.format(changeMasterCommand, storageNodeWCreds.getUser(), storageNodeWCreds.getPassword());
        LOG.info("Expecting for storage: " + storageNode + "; cmd = " + changeMasterCommandWithCred);
        EasyMock.expect(con.prepareStatement(changeMasterCommandWithCred)).andReturn(st).times(1);

        LOG.info("Expecting for storage: " + storageNode + "; cmd = " + RPL_DBUS_SET_CONFIG_RELOAD_STMT);
        EasyMock.expect(con.prepareStatement(RPL_DBUS_SET_CONFIG_RELOAD_STMT)).andReturn(st).times(1);
        LOG.info("Expecting for storage: " + storageNode + "; cmd = " + RPL_DBUS_START_SLAVE_STMT);
        EasyMock.expect(con.prepareStatement(RPL_DBUS_START_SLAVE_STMT)).andReturn(st).times(1);
        //EasyMock.expect(con.prepareStatement(EasyMock.<String>anyObject())).andReturn(st).anyTimes();

        EasyMock.replay(con);
      }
      catch (SQLException e)
      {
        throw new RplDbusException("Mocked connectToNodeFailed", e);
      }

      connectToNode(con, storageNode, offset);
    }

    @Override
    public void disconnectFromNode(RplDbusMysqlCoordinates storageNode) throws RplDbusException {
      _stateMap.put(RplDbusAdapter.createThreadNameForNode(storageNode), new RplDbusStateTest(false, storageNode));
    }

    public void setUseRealStates(boolean useReal) {
      _useRealStates = useReal;
    }
    @Override
    public Map<String, RplDbusState> getStates() throws RplDbusException {
      if(_useRealStates)
        return super.getStates();

     return _stateMap;
    }

    @Override
    public RplDbusState getState(RplDbusMysqlCoordinates storageNode) throws RplDbusException {
      return _stateMap.get(RplDbusAdapter.createThreadNameForNode(storageNode));
     }
  }

  //
  // fake RplDbusMasterState
  //
  public static class RplDbusMasterStateTest extends RplDbusMasterState {
    public void setPosition(int pos) {
      _position = pos;
    }
    public void setFile(String file) {
      _file = file;
    }
  }

  public static class RplDbusStateTest extends RplDbusState {
    private boolean _isUp;
    private final RplDbusMysqlCoordinates _mysql;

    public RplDbusStateTest(boolean up, RplDbusMysqlCoordinates node) {
      _isUp = up;
      _mysql = node;
      _masterHost = node.getHostname();
      _masterPort = node.getPort();
    }
    @Override
    public boolean isRplDbusUp() {
      return _isUp;
    }

    public void setUp(boolean up) {
      _isUp = up;
    }

    public void setMasterLogFile(String masterLogFile) {
      _master_Log_File = masterLogFile;
    }

    public void setRelayMasterLogFile(String relayMasterLogFile) {
      _relayMasterLogFile  = relayMasterLogFile;
    }

    public void setReadMasterLogPos(int masterLogPos) {
      _read_Master_Log_Pos = masterLogPos;
    }

    public  void setExecMasterLogPos(int execMasterLogPos) {
      _execMasterLogPos = execMasterLogPos;
    }
    /**
     * how far the IO thread on the slave go reading from the master
     * @return
     */
    @Override
    public int getExecMasterLogPos() {
      return _execMasterLogPos;
    }

  }

  /**
   * mock class for rpldbusMasterState
   */
  public static class RplDbusMasterStateTest1 extends RplDbusMasterState {
    // nothing - may need it to get test version of repl file
  }

  /**
   * mock class for RplDbusMasterAdapter
   *
   */
  public static class RplDbusMasterAdapterTest1 extends RplDbusMasterAdapter {

    public RplDbusMasterAdapterTest1(RplDbusMysqlCoordinatesWithCreds rNode) throws RplDbusException
    {
      super(rNode);
    }

    @Override
    public RplDbusMasterState getMasterState() {
      RplDbusMasterStateTest mState = new RplDbusMasterStateTest();
      mState.setServerId(112233);
      return mState;
    }
  }
}
