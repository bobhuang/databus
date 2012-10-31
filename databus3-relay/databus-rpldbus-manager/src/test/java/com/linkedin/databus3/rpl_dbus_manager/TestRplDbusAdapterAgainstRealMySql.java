package com.linkedin.databus3.rpl_dbus_manager;


import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus3.cm_utils.ClusterManagerUtilsException;

public class TestRplDbusAdapterAgainstRealMySql
{
  public static final String MODULE = TestRplDbusAdapterAgainstRealMySql.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  static
  {
    //PatternLayout defaultLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c} %m%n");
    PatternLayout defaultLayout = new PatternLayout("%m%n");
    ConsoleAppender defaultAppender = new ConsoleAppender(defaultLayout);

    Logger.getRootLogger().removeAllAppenders();
    Logger.getRootLogger().addAppender(defaultAppender);

    //Logger.getRootLogger().setLevel(Level.OFF);
    //Logger.getRootLogger().setLevel(Level.ERROR);


    //LOG.setLevel(Level.DEBUG);
    Logger l = Logger.getLogger(RplDbusManager.class.getName());
    l.setLevel(Level.DEBUG);
    Logger.getRootLogger().setLevel(Level.INFO);
    Logger.getRootLogger().setLevel(Level.DEBUG);
  }

  private RplDbusAdapter _rplDbusAdapter;
  private int _maxPort;

  // - should be called before tests
  public void setupConfig() throws RplDbusException {
    //... use tmp
    LOG.info("Starting test\n");
    RplDbusMysqlCoordinatesWithCreds node = new RplDbusMysqlCoordinatesWithCreds("localhost", 29000, "root", "");
    RplDbusManagerConfigBuilder cBuilder = new RplDbusManagerConfigBuilder();
    cBuilder.setStorageNodeMysqlUser("rplespresso");
    cBuilder.setStorageNodeMysqlPassword("espresso");
    cBuilder.setInstanceName("localhost_11140");
    try {
      cBuilder.setRelayRplDbusMapping("localhost_11140:localhost_29000");
    } catch (InvalidConfigException e) {}

    _rplDbusAdapter = new RplDbusAdapter(cBuilder.build(), node);
  }

  public void tearDown() throws RplDbusException {
    if(_rplDbusAdapter != null)
      _rplDbusAdapter.close();
  }

  /* this test runs against real instance - so the value
   * depend on the instance
   * USED FOR MANUAL testing only. Only requires a mysql rpldbus slave running on localhost:29000, user=root, ps=''
   */
  //@Test
  public void testAgainstLiveMysql() throws RplDbusException {
    testRplDbusAdapterState();
    testRplDbusAdapterConnect();
    testRplDbusAdapterDisconnect();
  }

  //@Test
  public void testStopAllThreads() throws RplDbusException {
    setupConfig();
    _rplDbusAdapter.stopAllThreads(true);
  }

  //@Test
  public void testRestartThread() throws RplDbusException {
    setupConfig();
    RplDbusMysqlCoordinates snMysql = new RplDbusMysqlCoordinates("localhost", 3306);
    _rplDbusAdapter.restartThread(snMysql, 12884902997L);
  }

  //@Test
  public void testExecuteStatements() throws RplDbusException {
    setupConfig();
    String [] stmts = new String [3];
    stmts[0] = "SHOW SLAVE STATUS";
    stmts[1] = "SHOW VARIABLES ";
    stmts[2] = "SHOW MASTER STATUS";

    _rplDbusAdapter.executeStatements(stmts);
  }

  //@Test
  public void testConfigureTcpPort() throws SQLException, RplDbusException {
    String url = "jdbc:mysql://localhost:29000/mysql?user=root";
    Connection conn = DriverManager.getConnection(url);
    int port = getTcpPort(conn);
    LOG.info("port = " + port);

    int newPort = port+100;


    setupConfig();
    // set a different port
    _rplDbusAdapter.configureTcpPort(newPort);
    int newNewPort = getTcpPort(conn);
    Assert.assertEquals(newNewPort, newPort);

    _rplDbusAdapter.configureTcpPort(port);
    newNewPort = getTcpPort(conn);
    Assert.assertEquals(newNewPort, port);


  }
  private int getTcpPort(Connection conn) throws SQLException {
    String query = "select value from mysql.rpl_dbus_config where prop_key='databus2.mysql.port'";
    PreparedStatement stmt = conn.prepareStatement(query);
    ResultSet rs = stmt.executeQuery();
    Assert.assertTrue(rs.next());
    int port = rs.getInt(1);
    rs.close();
    stmt.close();
    return port;
  }

  //@Test
  public void testRplDbusAdapterState() throws RplDbusException {
    LOG.info("Starting test State\n");
    setupConfig();

    Map<String, RplDbusState> states = _rplDbusAdapter.getStates();
    //Assert.assertEquals(states.size(), 2);
    for(Map.Entry<String, RplDbusState> state : states.entrySet()) {
      System.out.println("state: " + state.getKey() + "=" + state.getValue());
    }
    // negative test
    RplDbusState state = _rplDbusAdapter.getState(new RplDbusMysqlCoordinates("localhost", 100)); //fake
    Assert.assertNull(state);

    RplDbusMysqlCoordinates sn1 = new RplDbusMysqlCoordinates("localhost", 3306);
    state = _rplDbusAdapter.getState(sn1);
    LOG.info("for sn1 " + sn1 + " state=" + state);
    Assert.assertNotNull(state);
    Assert.assertTrue(state.isRplDbusUp());

    InetSocketAddress isa = new InetSocketAddress("localhost", 3306);
    RplDbusMysqlCoordinates sn2 = new RplDbusMysqlCoordinates(isa.getHostName(), isa.getPort());
    state = _rplDbusAdapter.getState(sn2);
    LOG.info("for sn2 " + sn2 + " state=" + state);
    Assert.assertNotNull(state);
    Assert.assertTrue(state.isRplDbusUp());

    tearDown();
  }


  //@Test
  public void testRplDbusAdapterConnect() throws RplDbusException {
    LOG.info("Starting test Connect\n");
    setupConfig();

    Map<String, RplDbusState> states = _rplDbusAdapter.getStates();
    int currentSize = states.size();
    int i = 0;
    _maxPort = 0;
    for(Map.Entry<String, RplDbusState> stateEntry : states.entrySet()) {
      String thName = stateEntry.getKey();
      RplDbusState state = stateEntry.getValue();
      System.out.println(i++  + ": for node " + thName + "=" + state);
      if(! state.isRplDbusUp())
        _rplDbusAdapter.connectToNode(new RplDbusMysqlCoordinates(state.getMasterHost(), state.getMasterPort()), 4295292479L);
      int port = state.getMasterPort();
      if(port > _maxPort) {
        _maxPort = port;
      }
    }

    // doesn't exist
    InetSocketAddress isa = new InetSocketAddress("localhost", ++_maxPort);
    RplDbusMysqlCoordinates sn1 = new RplDbusMysqlCoordinates(isa.getHostName(), isa.getPort());
    RplDbusState state = _rplDbusAdapter.getState(sn1);
    LOG.info("for sn1 (before connect) " + sn1 + " state=" + state);
    Assert.assertNull(state);

    // now connect it
    _rplDbusAdapter.connectToNode(sn1, 4295292479L);
    states = _rplDbusAdapter.getStates();
    Assert.assertEquals(states.size(), ++currentSize);
    String thName = RplDbusAdapter.createThreadNameForNode(sn1);
    state = states.get(thName);
    LOG.info("for sn1 " + sn1 + " state=" + state);
    Assert.assertNotNull(state);
    Assert.assertFalse(state.isRplDbusUp()); // cannot connect - so should be in the error state
    Assert.assertEquals(state.getMasterLogFile(), "mysql-bin.000001");
    Assert.assertEquals(state.getMasterLogPos(), 63039);

    tearDown();
  }

  // test - by looking at the logs
  //@Test
  public void testRplDbusAdapterDisconnectAgainstRealMysql() {
    RplDbusManagerConfigBuilder configBld = new RplDbusManagerConfigBuilder();
    configBld.setEnabled(true);

    RplDbusMysqlCoordinatesWithCreds rNode = new RplDbusMysqlCoordinatesWithCreds("localhost", 3306, "root", "");
    try {
      RplDbusAdapter adapter = new RplDbusAdapter(configBld.build(), rNode);
      adapter.disconnectFromNode(new RplDbusMysqlCoordinates("host",123));
    } catch (RplDbusException e) {  }
  }


  //@Test
  public void testRplDbusAdapterDisconnect() throws RplDbusException {
    LOG.info("Starting test Connect\n");
    setupConfig();
    Map<String, RplDbusState> states = _rplDbusAdapter.getStates();
    //int currentSize = states.size();
    // get all current threads
    int i = 0;
    for(Map.Entry<String, RplDbusState> state : states.entrySet()) {
      System.out.println(i++  + ": state: " + state.getKey() + "=" + state.getValue());
      int port = state.getValue().getMasterPort();
      if(port > _maxPort) {
        _maxPort = port;
      }
    }

    // doesn't exist
    InetSocketAddress isa = new InetSocketAddress("localhost", _maxPort);
    RplDbusMysqlCoordinates sn1 = new RplDbusMysqlCoordinates(isa.getHostName(), isa.getPort());
    RplDbusState state = _rplDbusAdapter.getState(sn1);
    LOG.info("for sn1 (before connect) " + sn1 + " state=" + state);
    Assert.assertNotNull(state);
    Assert.assertTrue(state.getSlaveSQLThreadRunning());
    //Assert.assertFalse(state.getSlaveIOThreadRunning());

    // now connect it

    _rplDbusAdapter.disconnectFromNode(sn1);
    states = _rplDbusAdapter.getStates();
    String thName = RplDbusAdapter.createThreadNameForNode(sn1);
    state = states.get(thName);
    LOG.info("for sn1 " + sn1 + " state=" + state);
    Assert.assertNotNull(state);
    Assert.assertFalse(state.isRplDbusUp()); // cannot connect - so should be in the error state
    Assert.assertFalse(state.getSlaveSQLThreadRunning());
    //Assert.assertFalse(state.getSlaveIOThreadRunning());
    tearDown();
  }

  //@Test
  // start thread, check, introduce an error, check again
  public void testErrorState() throws RplDbusException, InvalidConfigException, ClusterManagerUtilsException {
    setupConfig();
    Connection conn = null;
    try {

      String url = "jdbc:mysql://localhost:29000/mysql?user=root";
      try{
        conn = DriverManager.getConnection(url);

      } catch (SQLException e) {
        Assert.fail("execute sql command", e);
      }
      // change master
      String threadName = "localhost:3306";
      setThreadName(threadName, conn);
      stopThread(conn);
      changeMasterThread(conn, "127.0.0.1", "3306", "rplespresso", "espresso");
      startThread(conn);

      int i = 0;
      Map<String, RplDbusState> states = _rplDbusAdapter.getStates();
      for(Map.Entry<String, RplDbusState> state : states.entrySet()) {
        LOG.debug(i++  + ": before state: " + state.getKey() + "=" + state.getValue());
      }

      System.out.println("All threads should be UP");
      i=0;
      for(Map.Entry<String, RplDbusState> state : states.entrySet()) {
        LOG.debug(i++  + ": state: " + state.getKey() + "=" + state.getValue());
        if(state.getKey().equals(threadName)) {
          Assert.assertTrue(state.getValue().isRplDbusUp(), "thread1 is up");
        }
      }
      // put the thread into error state
      setThreadName(threadName, conn);
      stopThread(conn);
      changeMasterThread(conn, "0.0.0.1", "3306", "rplespresso", "espresso");
      startThread(conn);

      // verify that it is stopped
      states = _rplDbusAdapter.getStates();
      i = 0;
      System.out.println("All threads should be DOWN");
      for(Map.Entry<String, RplDbusState> state : states.entrySet()) {
        RplDbusState st = state.getValue();
        LOG.debug(i++  + ": state: " + state.getKey() + "=" + st);
        LOG.debug("io_error: " + st.getIOError() +  "sql_error: " + st.getSQLError() + "; err=" + st.isRplDbusError());
        if(threadName.equals(st.getLiRplSlaveName())) {
          Assert.assertFalse(st.isRplDbusUp());
          Assert.assertTrue(! st.getIOError().isEmpty());
        }
      }
    } finally {
      if(conn != null)
        try {
          conn.close();
        } catch (SQLException e) {
          Assert.fail("sql statement", e);
        }
    }
  }

  // get state, pick a thread , disable it, run validate, verify that it is enabled back
  //@Test
  public void testValidateCurrentState() throws RplDbusException, InvalidConfigException, ClusterManagerUtilsException {
    setupConfig();
    int i = 0;
    Map<String, RplDbusState> states = _rplDbusAdapter.getStates();
    for(Map.Entry<String, RplDbusState> state : states.entrySet()) {
      System.out.println(i++  + ": before state: " + state.getKey() + "=" + state.getValue());
    }

    // call validate -it supposed to stop all the thread (because the internal state is empty
    RplDbusManagerConfigBuilder config = new RplDbusManagerConfigBuilder();
    config.setEnabled(true);
    String instanceName = "localhost_11140";
    config.setInstanceName(instanceName);
    config.setRelayRplDbusMapping("localhost_11140:localhost_29000");

    RplDbusManager rdm = new RplDbusManager(config.build());
    //rdm.setNewRplDbusAdapter(new RplDbusNodeCoordinates("localhost", 100), _rplDbusAdapter);
    //rdm.setNewRplDbusAdapter(_rplDbusAdapter);
    RplDbusAdapter adapter = rdm.getRplDbusAdapter();

    // create fake mapping
    String masterHost = "eat1-app17.corp.linkedin.com";
    RplDbusMysqlCoordinates masterMysql = new RplDbusMysqlCoordinates( masterHost, 3306);
    RplDbusMysqlCoordinates localMasterMysql = new RplDbusMysqlCoordinates("localhost", 3306);

    System.out.println("Adding snMysql to the internal mapping: " + masterMysql);
    rdm._mySnViewMysql.put(masterMysql, masterMysql);
    rdm._mySnViewMysql.put(localMasterMysql, localMasterMysql);

    // no-op
    rdm.validateCurrentState();
    TestRplDbusManager.sleepSec(2);

    states = adapter.getStates();
    i = 0;
    boolean found1 = false;
    boolean found2 = false;
    String threadName1 = RplDbusAdapter.createThreadNameForNode(masterMysql);
    String threadName2 = "localhost:3306";
    System.out.println("All threads should be UP");
    for(Map.Entry<String, RplDbusState> state : states.entrySet()) {
      System.out.println(i++  + ": state: " + state.getKey() + "=" + state.getValue());
      if(state.getKey().equals(threadName1)) {
        found1 = true;
        Assert.assertTrue(state.getValue().isRplDbusUp(), "thread1 is up");
      }
      if(state.getKey().equals(threadName2)) {
        found2 = true;
        Assert.assertTrue(state.getValue().isRplDbusUp(), "thread2 is up");
      }
    }
    Assert.assertTrue(found1, " eat1-app13 thread found");
    Assert.assertTrue(found2, " eat1-app13 thread found");
    // try stopping one of the threads
    String url = "jdbc:mysql://localhost:29000/mysql?user=root";
    Connection conn = null;
    try {
      try{
        conn = DriverManager.getConnection(url);
      } catch (SQLException e) {
        Assert.fail("execute sql command", e);
      }

      setThreadName(threadName1, conn);
      stopThread(conn);
      setThreadName(threadName2, conn);
      stopThread(conn);
    } finally {
      if(conn != null)
        try {
          conn.close();
        } catch (SQLException e) {
          Assert.fail("sql statement", e);
        }
    }

    // verify that it is stopped
    states = adapter.getStates();
    i = 0;
    System.out.println("All threads should be DOWN");
    for(Map.Entry<String, RplDbusState> state : states.entrySet()) {
      RplDbusState st = state.getValue();
      LOG.debug(i++  + ": state: " + state.getKey() + "=" + st);
      if(threadName1.equals(st.getLiRplSlaveName()) || threadName2.equals(st.getLiRplSlaveName())) {
        Assert.assertFalse(st.isRplDbusUp());
      }
    }

    // run validate
    rdm.validateCurrentState();
    TestRplDbusManager.sleepSec(1);
    // check again

    states = adapter.getStates();
    i = 0;
    System.out.println("All threads should be UP");
    for(Map.Entry<String, RplDbusState> state : states.entrySet()) {
      RplDbusState st = state.getValue();
      System.out.println(i++  + ": state: " + state.getKey() + "=" + st);
      if(threadName1.equals(st.getLiRplSlaveName()) || threadName2.equals(st.getLiRplSlaveName()))
        Assert.assertTrue(st.isRplDbusUp());
    }

    tearDown();
  }

  @Test
  public void testPrepareChangeMasterStatment() {
    // create statement without offset
    RplDbusMysqlCoordinatesWithCreds node = new RplDbusMysqlCoordinatesWithCreds("someHost", 100, "user", "pw");
    String stmt = RplDbusAdapter.prepareChangeMasterStatment(node, -1);
    String expected = "change master to master_host='someHost',master_port=100,master_user='%s',master_password='%s'";

    Assert.assertEquals(stmt, expected);
    long offset = 4295292479L;
    stmt = RplDbusAdapter.prepareChangeMasterStatment(node, offset);
    expected = "change master to master_host='someHost',master_port=100,master_user='%s',master_password='%s', MASTER_LOG_FILE='mysql-bin.000001', MASTER_LOG_POS=325183;";
    //System.out.println(stmt);

    Assert.assertEquals(stmt, expected);
  }

  @Test
  public void testBuildUrl() throws UnknownHostException {
    // build url for connectiong to mysql
    RplDbusMysqlCoordinatesWithCreds node = new RplDbusMysqlCoordinatesWithCreds("localhost", 14100, "espresso", "espresso");
    String url = RplDbusAdapter.buildUrl(node);
//    Assert.assertEquals(url, "jdbc:mysql://localhost:14100/mysql?user=%s&password=%s");
    String urlCred = String.format(url, node.getUser(), node.getPassword());
    //String resolvedLocalhost = InetAddress.getLocalHost().getCanonicalHostName();
    Assert.assertEquals(urlCred, "jdbc:mysql://localhost:14100/mysql?user=espresso&password=espresso");
  }

  // runs agains real myslq instance - so some values come from there..
  //@Test
  public void testRplDbusMasterAdapter() throws RplDbusException {
    RplDbusMysqlCoordinatesWithCreds node = new RplDbusMysqlCoordinatesWithCreds("localhost", 14100, "espresso", "espresso");
    RplDbusMasterAdapter master = new RplDbusMasterAdapter(node);
    RplDbusMasterState mState = master.getMasterState();
    System.out.println("master state = " + mState);
    Assert.assertEquals(mState.getServerId(), "14100");
    Assert.assertTrue(mState.getFile().startsWith("mysql-bin"));
  }

  public static void setThreadName(String threadName, Connection conn) {
    PreparedStatement stmt = null;
    try{
      String query = "set global li_rpl_slave_name='" + threadName + "';";
      LOG.info("About to set global " + threadName);
      stmt = conn.prepareStatement(query);
      stmt.executeQuery();
      stmt.close();

    } catch (SQLException e) {
      Assert.fail("execute sql command", e);
    } finally {
      if(stmt != null)
        try {
          stmt.close();
        } catch (SQLException e) {
          Assert.fail("sql statement", e);
        }
    }
  }

  public static void startThread(Connection conn) {
    executeQuery("start slave", conn);
  }
  public static void stopThread(Connection conn) {
    executeQuery("stop slave", conn);
  }

  public static void changeMasterThread(Connection conn, String host, String port, String user, String pw) {
    String query = "change master to master_host='" + host + "',master_port=" + port + ",master_user='" + user + "',master_password='" + pw + "';";
    executeQuery(query, conn);
  }

  private static void executeQuery(String query, Connection conn) {
    PreparedStatement stmt = null;
    try{
      LOG.info("about to " + query);
      stmt = conn.prepareStatement(query);
      stmt.executeQuery();
      stmt.close();
    } catch (SQLException e) {
      Assert.fail("execute sql command", e);
    } finally {
      if(stmt != null)
        try {
          stmt.close();
        } catch (SQLException e) {
          Assert.fail("sql statement", e);
        }
    }
  }
}

