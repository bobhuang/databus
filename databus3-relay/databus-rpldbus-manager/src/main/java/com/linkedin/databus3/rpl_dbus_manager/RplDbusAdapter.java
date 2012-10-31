package com.linkedin.databus3.rpl_dbus_manager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.monitoring.mbean.StatsCollectors;
import com.linkedin.databus3.rpl_dbus_manager.monitoring.mbean.RplDbusTotalStats;

public class RplDbusAdapter
{

  private static final Logger LOG = Logger.getLogger(RplDbusAdapter.class.getName());
  private static final int RPL_DBUS_connNECTION_TIMEOUT_SEC = 2;
  static final String RPL_DBUS_DISCONNECT_HOST = "li.null";
  static final String RPL_DBUS_STATUS_STMT = "SHOW SLAVE STATUS";
  static final String RPL_DBUS_STOP_SLAVE_STMT = "STOP SLAVE";
  static final String RPL_DBUS_START_SLAVE_STMT = "START SLAVE";
  static final String RPL_DBUS_RESET_SLAVE_STMT = "RESET SLAVE";
  static final String RPL_DBUS_SET_SLAVE_NAME_STMT = " set global Li_rpl_slave_name='%s'";
  static final String RPL_DBUS_SET_CHANGE_MASTER_TEMPLATE_STMT =
      "change master to master_host='%s',master_port=%s,master_user='%s',master_password='%s'";
  static final String RPL_DBUS_SET_TCP_PORT_STMT =
      "insert into mysql.rpl_dbus_config values ('databus2.mysql.port','%d') on duplicate key update value='%d'";
  static final String RPL_DBUS_SET_CONFIG_RELOAD_STMT = "set global rpl_dbus_request_config_reload=1;";

  private static final String RPL_DBUS_SET_CHANGE_MASTER_WITH_OFFSET_TEMPLATE_STMT =
      "change master to master_host='%s',master_port=%s,master_user='%s',master_password='%s'," +
      " MASTER_LOG_FILE='%s', MASTER_LOG_POS=%s;";

  private static final String RPL_DBUS_REPLICATION_FILE_PREFIX = "mysql-bin";

  private final RplDbusMysqlCoordinatesWithCreds _thisRplDbusNode;
  private final StatsCollectors<RplDbusTotalStats> _stats;
  //private RplDbusMonitor _monitor;
  private Connection _conn;  // jdbc connection
  private final String _url; // url for the connection

  private String _snUser = "root";
  private String _snPassword = "";


  // maps li_rpl_slave_name to the state
  private final Map<String, RplDbusState> _map =
      new HashMap<String, RplDbusState>();

  public RplDbusAdapter(RplDbusManagerStaticConfig config, RplDbusMysqlCoordinatesWithCreds rNode)
      throws RplDbusException {
    this(config, rNode, null);
  }
  /**
   * connects to one specific rpl_dbus instance
   * @param rNode
   * @throws RplDbusException
   */
  public RplDbusAdapter(RplDbusManagerStaticConfig config, RplDbusMysqlCoordinatesWithCreds rNode, StatsCollectors<RplDbusTotalStats> stats)
      throws RplDbusException {

    _thisRplDbusNode = rNode;
    _stats = stats;

    _snUser = config.getStorageNodeMysqlUser();
    _snPassword = config.getStorageNodeMysqlPassword();

    // build the url to connect
    _url = buildUrl(_thisRplDbusNode);
    LOG.info("building connection to RPL_DBUS. url=" + _url);
    try
    {
      Class.forName("com.mysql.jdbc.Driver");
    }
    catch(ClassNotFoundException e)
    {
      throw new RplDbusException("Error loading jdbc Driver", e);
    }

    // create the connection and keep it around
    connect();
  }

  /**
   * configures rpldbus to connect to this tcp port
   * @param port
   * @throws RplDbusException
   */
  public void configureTcpPort(int port) throws RplDbusException {
    connect();

    // prepare statements
    LOG.info("configuring rpldbus to connect to tcp port=" + port + ", using following statments:");
    String setPortCommand = String.format(RPL_DBUS_SET_TCP_PORT_STMT, port, port);
    PreparedStatement stmt = null;
    try{
      stmt = _conn.prepareStatement(setPortCommand);
      LOG.info("\t" + setPortCommand);
      stmt.executeUpdate();
      stmt.close();
      stmt = _conn.prepareStatement(RPL_DBUS_SET_CONFIG_RELOAD_STMT);
      LOG.info("\t" + RPL_DBUS_SET_CONFIG_RELOAD_STMT);
      stmt.executeQuery();
      stmt.close();
    } catch (SQLException e){
      LOG.error("failed to configure port with " + setPortCommand, e);
    } finally {
      try {
        if(stmt != null)
          stmt.close();
      } catch (SQLException e) {
        LOG.error("failed to close statement",e);
      }
    }
  }

  public RplDbusMasterAdapter createRplDbusMasterAdapter(RplDbusMysqlCoordinates mysql) throws RplDbusException {
    RplDbusMysqlCoordinatesWithCreds mysqlWithCred = new RplDbusMysqlCoordinatesWithCreds(mysql.getHostname(),
                                                                                        mysql.getPort(),
                                                                                        _snUser,
                                                                                        _snPassword);
    return new RplDbusMasterAdapter(mysqlWithCred);
  }

  static String buildUrl(RplDbusMysqlCoordinatesWithCreds node) {
    StringBuilder sb = new StringBuilder("jdbc:mysql://");
    sb.append(node.getHostname()).append(":").append(node.getPort());
    sb.append("/mysql?user=%s");
    sb.append("&password=%s");

    return sb.toString();
  }

  public void close() throws RplDbusException {
    if(_conn != null)
      try
      {
        _conn.close();
      }
      catch (SQLException e)
      {
        String msg = "failed to close connection to RPL_DBUS. con = " + _conn;
        LOG.error(msg);
        throw new RplDbusException(msg, e);      }
  }


  // connect if not connected
  public void connect() throws RplDbusException {
    String url = String.format(_url, _thisRplDbusNode.getUser(), _thisRplDbusNode.getPassword());
    try
    {
      if(_conn != null  && _conn.isValid(RPL_DBUS_connNECTION_TIMEOUT_SEC)) {
        LOG.info("connection is still valid:" + _url + " " + _thisRplDbusNode.getUser() + "/" + _thisRplDbusNode.getPassword());
        return; // nothing to do
      }
      // put in the credentials
      if(_conn != null) // open but not valid
        _conn.close();
      _conn = DriverManager.getConnection(url);
      if(_stats != null) { // update stats
        for(RplDbusTotalStats stat : _stats.getStatsCollectors())
          stat.registerConnectTime(System.currentTimeMillis());
      }
    } catch (SQLException e) {
      String msg = "failed to connect to RPL_DBUS. Url = " + url;
      LOG.error(msg);
      throw new RplDbusException(msg, e);
    }
    LOG.info("Create new connection to RPLDbus = " + _url);
  }

  /*
  private boolean isConnectionValid() {
    //return _conn.isValid(RPL_DBUS_connNECTION_TIMEOUT_SEC);
    return true;
  }
  */
  public static String createThreadNameForNode(RplDbusNodeCoordinates storageNode) {
    return storageNode.getHostname().replace('.', '_') + ":" + storageNode.getPort();
  }

  public static int getFileNameIndexFromBinlogOffset(long offset) {
    return (int) (offset>>32);
  }
  public static int getFileOffsetFromBinlogOffset(long offset) {
    return (int)(offset&0xffffffffL);
  }

  /**
   * build statement for changing the master
   * @param storageNode
   * @param offset
   * @return statement as a String
   */
  static String prepareChangeMasterStatment(RplDbusMysqlCoordinatesWithCreds storageNode, long offset) {
    // rpldbus doesn't accept localhost as the host name so we need to change it HACK
    String snHostName = RplDbusNodeCoordinates.convertLocalHostToFqdn(storageNode.getHostname());
    if(offset<=0) //just reset
      return String.format(RPL_DBUS_SET_CHANGE_MASTER_TEMPLATE_STMT,
                           snHostName,
                           storageNode.getPort(),
                           "%s",
                           "%s");

    // reset to a specific value
    // figure out the file name and the offset
    // first byte - is the file number
    int fileNum = getFileNameIndexFromBinlogOffset(offset);

    int fileOffset = getFileOffsetFromBinlogOffset(offset);

    String f = String.format("%06d", fileNum);
    String fileName = RPL_DBUS_REPLICATION_FILE_PREFIX + "." + f;
    return String.format(RPL_DBUS_SET_CHANGE_MASTER_WITH_OFFSET_TEMPLATE_STMT,
                         snHostName,
                         storageNode.getPort(),
                         "%s",
                         "%s",
                         fileName,
                         fileOffset);
  }

  public void stopAllThreads(boolean reset) throws RplDbusException {
    Map<String, RplDbusState> map = getStates();
    for(Map.Entry<String, RplDbusState> e : map.entrySet()) {
      String threadName = e.getKey();
      stopStartResetSlaveByName(threadName, RPL_DBUS_STOP_SLAVE_STMT);
      if(reset)
        stopStartResetSlaveByName(threadName, RPL_DBUS_RESET_SLAVE_STMT);
    }
  }

  /**
   * perform stop on slave
   * @param snMysql
   * @throws RplDbusException
   *
  public void stopSlave(RplDbusMysqlCoordinates snMysql) throws RplDbusException {
    stopStartResetSlave(snMysql, RPL_DBUS_STOP_SLAVE_STMT);
  }
  **
   * perform start on slave
   * @param snMysql
   * @throws RplDbusException
   *
  public void startSlave(RplDbusMysqlCoordinates snMysql) throws RplDbusException {
    stopStartResetSlave(snMysql, RPL_DBUS_START_SLAVE_STMT);
  }
  **
   * perform rest on slave
   * @param snMysql
   * @throws RplDbusException
   */
  public void resetSlave(RplDbusMysqlCoordinates snMysql) throws RplDbusException {
    stopStartResetSlave(snMysql, RPL_DBUS_RESET_SLAVE_STMT);
  }

  // issue stop/start/reset command to the thread
  private void stopStartResetSlave(RplDbusMysqlCoordinates snMysql, String cmd) throws RplDbusException {
    connect(); //make sure connection is on

    // create THREAD name
    String threadName = createThreadNameForNode(snMysql);
    stopStartResetSlaveByName(threadName, cmd);
  }

  private void stopStartResetSlaveByName(String threadName, String cmd) throws RplDbusException {

    // prepare statements
    LOG.info("about to " + cmd + " SN (tn=" + threadName + "), over conn = " + _conn + " using following statments:");
    String [] stmts = new String[2];
    String setSlaveName = String.format(RPL_DBUS_SET_SLAVE_NAME_STMT, threadName);
    stmts[0] = setSlaveName;
    stmts[1] = cmd;
    executeStatements(stmts);
  }

   void executeStatements(String [] statements) throws RplDbusException {
    PreparedStatement stmt = null;
    ResultSet rs = null;
    for(String stmtStr : statements) {
      if(stmtStr == null || stmtStr.equals(""))
        continue;
      try {
        stmt = _conn.prepareStatement(stmtStr);
        LOG.info("\t" + stmtStr);
        stmt.executeQuery();
        stmt.close();
        // for debugging print show warnings
        stmt = _conn.prepareStatement("SHOW WARNINGS");
        rs = stmt.executeQuery();
        while(rs.next()) { // should be empty if all ok
          LOG.info("Warning for statement " + stmtStr +  ":");
          LOG.info("\t" + rs.getString(1) + "," + rs.getString(2) + "," + rs.getString(3));
        }
        rs.close();
        stmt.close();
      } catch (SQLException e) {
        throw new RplDbusException("failed mysql statement: " + stmtStr, e);
      }finally {
        if(rs != null) {
          try {
            rs.close();
          } catch (SQLException e){
            LOG.warn("failed to close result set", e);
          }
        }
        if(stmt != null) {
          try {
            stmt.close();
          } catch (SQLException e){
            LOG.warn("failed to close stmt", e);
          }
        }
      }
    }
  }

  /**
   * connect to a strage Node (mysql instance)
   * @param storageNode
   * @param binlogOffset
   * @throws RplDbusException
   */
  public void connectToNode(RplDbusMysqlCoordinates storageNode, long binlogOffset) throws RplDbusException {
    connectToNode(_conn, storageNode, binlogOffset);
  }

  void connectToNode(Connection conn, RplDbusMysqlCoordinates storageNode, long binlogOffset) throws RplDbusException {
    connect(); //make sure connection is on

    // create THREAD name
    String threadName = createThreadNameForNode(storageNode);
    RplDbusMysqlCoordinatesWithCreds storageNodeWCreds = new RplDbusMysqlCoordinatesWithCreds(
               storageNode.getHostname(), storageNode.getPort(), _snUser, _snPassword);

    // prepare statments
    PreparedStatement stmt = null;
    try {
      LOG.info("about to connect to SN (tn=" + threadName + "), over conn = " + conn + " using following statments:");
      // select slave
      String formatedCommand = String.format(RPL_DBUS_SET_SLAVE_NAME_STMT, threadName);
      stmt = conn.prepareStatement(formatedCommand);
      LOG.info("\t" + formatedCommand);
      stmt.executeQuery();
      stmt.close();
      // stop slave
      stmt = conn.prepareStatement(RPL_DBUS_STOP_SLAVE_STMT);
      LOG.info("\t" + RPL_DBUS_STOP_SLAVE_STMT);
      stmt.executeQuery();
      stmt.close();
      // set new master
      String changeMasterStmt = prepareChangeMasterStatment(storageNodeWCreds, binlogOffset);
      String changeMasterStmtWithCred = String.format(changeMasterStmt, storageNodeWCreds.getUser(), storageNodeWCreds.getPassword());
      stmt = conn.prepareStatement(changeMasterStmtWithCred);
      LOG.info("\t" + changeMasterStmtWithCred);
      stmt.executeQuery();
      stmt.close();
      // start slave
      stmt = conn.prepareStatement(RPL_DBUS_START_SLAVE_STMT);
      LOG.info("\t" + RPL_DBUS_START_SLAVE_STMT);
      stmt.executeQuery();
      stmt.close();
    }
    catch (SQLException e) {
      throw new RplDbusException("faild mysql statement: " + stmt, e);
    }finally {
      if(stmt != null)
        try {
          stmt.close();
        } catch (SQLException e){
          LOG.warn("failed to close stmt");
        }
    }
  }

  /**
   * disconnects the thread for the storageNode
   * NOTE. it does NOT kill the thread (api not available yet) // TBD
   * @param storageNode
   * @throws RplDbusException
   */
  public void disconnectFromNode(RplDbusMysqlCoordinates storageNode) throws RplDbusException {
    connect(); //make sure connection is on

    // create THREAD name
    String threadName = createThreadNameForNode(storageNode);

    // prepare statments
    PreparedStatement stmt = null;
    try {
      LOG.info("about to disconnect  SN (tn=" + threadName + ") using following statments:");
      // select slave - using 'set global li_rpl_slave_name threadName'
      stmt = _conn.prepareStatement(String.format(RPL_DBUS_SET_SLAVE_NAME_STMT, threadName));
      LOG.info("\t" + stmt.toString());
      stmt.executeQuery();
      stmt.close();
      // stop slave
      stmt = _conn.prepareStatement(RPL_DBUS_STOP_SLAVE_STMT);
      LOG.info("\t" + stmt.toString());
      stmt.executeQuery();
      stmt.close();
      // we need to set 'linull' master, port(0), offset (-1), username and password ("") don't matter
      RplDbusMysqlCoordinatesWithCreds disconnectNode = new RplDbusMysqlCoordinatesWithCreds(RPL_DBUS_DISCONNECT_HOST, 0, "", "");
      String stmtString = prepareChangeMasterStatment(disconnectNode, -1);
      LOG.info("\t" + stmtString);
      String stmtWithCred = String.format(stmtString, disconnectNode.getUser(), disconnectNode.getPassword());
      stmt = _conn.prepareStatement(stmtWithCred);
      stmt.executeQuery();
      stmt.close();
    }
    catch (SQLException e) {
      throw new RplDbusException("faild mysql statement: " + stmt, e);
    } finally {
      if(stmt != null) {
        try {
          stmt.close();
        } catch (SQLException e) {
          LOG.error("failed to close stmt", e);
        }
      }
    }
  }

  public void restartThread(RplDbusMysqlCoordinates snMysql, long binlogOffset) throws RplDbusException {
    LOG.info("about to reconnect/restart to : " + snMysql + " for offset = " + binlogOffset);
    connectToNode(snMysql, binlogOffset);
  }

  /**
   *  get rpl state for single storage node
   * @param storageNode
   * @return RplDbusState for the storage node
   * @throws RplDbusException
   */
  public RplDbusState getState(RplDbusMysqlCoordinates snMysql) throws RplDbusException {
    Map<String, RplDbusState> map = getStates();

    String threadName = createThreadNameForNode(snMysql);
    return map.get(threadName);
  }

  /**
   *
   * @return map between storage machine and corresponding thread info in RPL_DBUS
   * @throws RplDbusException
   */
  public Map<String, RplDbusState> getStates() throws RplDbusException {

    // verify connection
    connect();

    _map.clear();

    PreparedStatement stmt = null;
    ResultSet rs = null;
    try
    {
      stmt = _conn.prepareStatement(RPL_DBUS_STATUS_STMT);
      stmt.executeQuery();
      rs = stmt.getResultSet();

      while(rs.next()) {
        RplDbusState rsState = new RplDbusState(rs);
        LOG.debug("getStates: rs = " + rs.getString(RplDbusState.SLAVE_NAME));
        _map.put(rsState.getLiRplSlaveName(), rsState);
      }
    }
    catch (SQLException e)
    {
      String msg = "cannot execute query on RPL_DBUS. stmt = " + stmt;
      LOG.error(msg);
      throw new RplDbusException(msg, e);
    } finally {
      if(stmt != null) {
        try {
          stmt.close();
        } catch (SQLException e) {
          LOG.error("failed to close stmt", e);
        }
      }
      if(rs != null)
        try {
          rs.close();
        } catch (SQLException e) {
          LOG.error("Failed to close result set",e);
        }
    }

    if(_map.size() < 1) {
      // this is valid case if there is not threads
      //throw new RplDbusException("result set is empty for url" + _url);
    }

    // just to get better estimate
    return _map;
  }


   /*
  public RplDbusNode getCurrentlyConnectedMaster() throws RplDbusException {

    String masterHost;
    int masterPort;
    try {
      masterHost = _monitor.getMasterHost();
      masterPort = _monitor.getMasterPort();
    } catch (RplDbusException de){
      String msg = "Cannot get info from RPL_DBUS";
      LOG.error(msg, de);
      throw new RplDbusException(msg, de);
    }

    return new RplDbusNode(masterHost, masterPort);
  }
  */

  public RplDbusMysqlCoordinatesWithCreds getNodeCoordinates() {
    return _thisRplDbusNode;
  }

  @Override
  public String toString() {
    return _url;
  }

  public static void main (String [] args) throws RplDbusException {
    if(args.length < 2) {
      System.out.println("Usage: rpldbus hostname port [user] [pw]");
      System.exit(-1);
    }
    String host = args[0];
    int port = Integer.parseInt(args[1]);
    String user = "root";
    String pw = "";
    if(args.length >= 3) {
      user = args[2];
    }
    if(args.length >= 4) {
      pw = args[3];
    }

    RplDbusMysqlCoordinatesWithCreds node = new RplDbusMysqlCoordinatesWithCreds(host, port, user, pw);

    System.err.println("Connecting to node:"  + node);

    RplDbusManagerConfigBuilder cBuilder = new RplDbusManagerConfigBuilder();
    RplDbusAdapter rplDbusAdapter = new RplDbusAdapter(cBuilder.build(), node);
    Map<String, RplDbusState> map = rplDbusAdapter.getStates();

    // printinng
    //String head = "NODE PORT \t SLAVE_NAME MASTERHOST MASTERPORT M_SID MASTER_LOG_FILE POS SQLT IOTH ERROR";
    String format = "%-30.30s\t %-25.25s %-5.5s %-5.5s %-20.20s %-4.4s %-4.4s %-4.4s %-10.10s ";
    String head = String.format(format,"THREAD_NAME","MASTER_HOST","MPORT","M_SID",
                                "MASTER_LOG_FILE","POS","SQLT","IOTH","ERROR");
    System.out.println(head);
    for(Map.Entry<String, RplDbusState> e : map.entrySet()) {
      String threadName = e.getKey();
      RplDbusState s = e.getValue();
      String state = String.format(format,
                                   threadName,
                                   s.getMasterHost(),
                                   "" + s.getMasterPort(),
                                   "" + s.getMasterServerid(),
                                   s.getMasterLogFile(),
                                   "" + s.getMasterLogPos(),
                                   s.getSlaveSQLThreadRunning()?"TRUE":"FLSE",
                                   s.getSlaveIOThreadRunning()?"TRUE":"FLSE",
                                   "" + s.isRplDbusError()
                                       );
      System.out.println(state);
    }
  }
}
