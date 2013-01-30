package com.linkedin.databus3.rpl_dbus_manager;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/



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
