package com.linkedin.databus3.rpl_dbus_manager;


import java.sql.ResultSet;
import java.sql.SQLException;


public class RplDbusMasterState {

  protected String _file;
  protected int _position;
  protected int _serverId=-1;

  public RplDbusMasterState() {

  }

  public void setServerId(int id) {
    _serverId = id;
  }
  public int getServerId() {
    return _serverId;
  }

  public RplDbusMasterState(ResultSet rs) throws RplDbusException {
    try
    {
      if(rs == null || rs.isClosed())
        throw new RplDbusException("invalid Result Set for RplDbusState", null);

      _file = rs.getString(1);
      _position = rs.getInt(2);
    } catch (SQLException e) {
      String msg = "invalid result set";
      throw new RplDbusException(msg, e);
    }
  }

  public String getFile() {
    return _file;
  }

  public int getPosition() {
    return _position;
  }
  @Override
  public String toString() {
    return "serverId= " + _serverId + "; masterFile=" + _file + "; masterPosition=" + _position;
  }
}