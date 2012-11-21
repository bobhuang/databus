package com.linkedin.databus3.rpl_dbus_manager;

import java.sql.ResultSet;
import java.sql.SQLException;


public class RplDbusState {
  private static int MASTER_HOST = 2;
  private static int MASTER_PORT = 4;
  private static int MASTER_LOG_FILE = 6;           //!< File on master that the rpldbus is currently reading from
  private static int READ_MASTER_LOG_FILE_POS = 7;  //!< How far rpldbus has read from the master file
  private static int RELAY_MASTER_LOG_FILE = 10;    //!< Current file that rpldbus is pushing to relay
  private static int IO_THREAD = 11;
  private static int SQL_THRED = 12;
  private static int EXEC_MASTER_LOG_FILE_POS = 22;//!< How far rpldbus has pushed (to relay) from the current file
  private static int SECONDS_BEHIND_MASTER = 33;

  private static int IO_ERROR_NO = 35;
  private static int IO_ERROR = 36;

  private static int SQL_ERROR_NO = 37;
  private static int SQL_ERROR = 38;

  private static int MASTER_ID = 40;
  static int SLAVE_NAME = 41;
  private static int REPL_DB = 42;

  private long _updateTime;

  // state fields
  protected boolean _slaveIOThreadRunning;
  protected boolean _slaveSQLThreadRunning;
  protected int _secondsBehindMaster;
  protected int _masterServerId;
  protected String _li_rpl_slave_name;
  protected String _li_rpl_replicate_db;
  protected String _master_Log_File;
  protected String _relayMasterLogFile;
  protected int _read_Master_Log_Pos;
  protected int _execMasterLogPos;

  protected String _masterHost;
  protected int _masterPort;
  String _ioError;
  int _ioErrorNo;
  String _sqlError;
  int _sqlErrorNo;

  public RplDbusState() {

  }

  public RplDbusState(ResultSet rs) throws RplDbusException {
    try
    {
      if(rs == null || rs.isClosed())
        throw new RplDbusException("invalid Result Set for RplDbusState", null);

      _masterHost = rs.getString(MASTER_HOST);
      _masterPort = rs.getInt(MASTER_PORT);
      _master_Log_File = rs.getString(MASTER_LOG_FILE);
      _read_Master_Log_Pos = rs.getInt(READ_MASTER_LOG_FILE_POS);
      _relayMasterLogFile = rs.getString(RELAY_MASTER_LOG_FILE);
      _execMasterLogPos = rs.getInt(EXEC_MASTER_LOG_FILE_POS);
      _slaveIOThreadRunning = "Yes".equals(rs.getString(IO_THREAD));
      _slaveSQLThreadRunning = "Yes".equals(rs.getString(SQL_THRED));
      _secondsBehindMaster = rs.getInt(SECONDS_BEHIND_MASTER);
      _masterServerId = rs.getInt(MASTER_ID);
      _li_rpl_slave_name = rs.getString(SLAVE_NAME);
      _li_rpl_replicate_db = rs.getString(REPL_DB);
      _ioError = rs.getString(IO_ERROR);
      _ioErrorNo = rs.getInt(IO_ERROR_NO);
      _sqlError = rs.getString(SQL_ERROR);
      _sqlErrorNo = rs.getInt(SQL_ERROR_NO);

    } catch (SQLException e) {
      String msg = "invalid result set";
      throw new RplDbusException(msg, e);
    }

    _updateTime = System.currentTimeMillis();

  }
  public String getLiRplSlaveName() {
    return _li_rpl_slave_name;
  }

  public String getMasterLogFile() {
    return _master_Log_File;
  }

  public String getRelayMasterLogFile() {
    return _relayMasterLogFile;
  }

  public int getMasterLogPos() {
    return _read_Master_Log_Pos;
  }
  /**
   * how far the IO thread on the slave go reading from the master
   * @return
   */
  public int getExecMasterLogPos() {
    return _execMasterLogPos;
  }


  public boolean isRplDbusUp() {
    return _slaveIOThreadRunning && _slaveSQLThreadRunning;
  }

  public boolean isRplDbusError() {
    return (_ioError != null && !_ioError.isEmpty()) ||
        (_sqlError != null && !_sqlError.isEmpty());
  }

  public String getIOError() {
    return (_ioError == null) ? "" : _ioError;
  }

  public String getSQLError() {
    return (_sqlError == null) ? "" : _sqlError;
  }

  public String getMasterHost() {
    return _masterHost;
  }

  public int getMasterPort() {
    return _masterPort;
  }

  public int getMasterServerid() {
    return _masterServerId;
  }

  public long getSecondsBehindMaster() {
    return _secondsBehindMaster;
  }

  public long secFromUpdate() {
    return System.currentTimeMillis() - _updateTime;
  }

  public boolean getSlaveSQLThreadRunning() {
    return _slaveSQLThreadRunning;
  }

  public boolean getSlaveIOThreadRunning() {
    return _slaveIOThreadRunning;
  }

  /**
   * @param node
   * @return true if rpldbus is connected to this node
   */
  public boolean connectedToHost(RplDbusNodeCoordinates node) {
    String hostname = node.getHostname();
    if(hostname.equals("localhost"))  {// HACK - rpldbus doesn't work with localhost
      hostname = RplDbusNodeCoordinates.convertLocalHostToFqdn(hostname);
      RplDbusManager.LOG.warn("converting localhost to " + hostname);
    }

    return hostname.equals(_masterHost) && node.getPort() == _masterPort;
  }

  @Override
  public String toString() {
    return "[masterHost=" + getMasterHost() + ":" + _masterPort +
        ",masterServerID=" + _masterServerId +
        ",slaveIOThreadRunning=" + _slaveIOThreadRunning +
        ",readPosition=" + _master_Log_File + ":" + _read_Master_Log_Pos +
        ",slaveSQLThreadRunning=" + _slaveSQLThreadRunning +
        ",writePosition=" + _relayMasterLogFile + ":" + _execMasterLogPos +
        ",secondsBehindMaster=" + _secondsBehindMaster +
        ",_li_rpl_slave_name=" + _li_rpl_slave_name +
        ((_ioError == null || _ioError.isEmpty()) ? "" : ",IO_ERROR:" + _ioError) +
        ((_sqlError == null || _sqlError.isEmpty()) ? "" : ",SQL_ERROR:" + _sqlError) +
        "]";
  }
}
