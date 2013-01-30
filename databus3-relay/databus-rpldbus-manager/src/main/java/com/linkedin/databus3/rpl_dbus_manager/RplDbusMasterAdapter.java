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


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

public class RplDbusMasterAdapter
{
  private static final Logger LOG = Logger.getLogger(RplDbusAdapter.class.getName());
  
  private static final String RPL_DBUS_STATUS_STMT = "SHOW MASTER STATUS";
  private static final String RPL_DBUS_SEVER_ID_STMT = "SELECT @@server_id;";
  
  private final RplDbusMysqlCoordinatesWithCreds _thisRplDbusNode;
  //private RplDbusMonitor _monitor;
  private Connection _conn;  // jdbc connection
  private final String _url; // url for the connection
  
  /**
   * connects to one specific rpl_dbus instance
   * @param rNode
   * @throws RplDbusException
   */
  public RplDbusMasterAdapter(RplDbusMysqlCoordinatesWithCreds rNode) 
      throws RplDbusException {
    
    _thisRplDbusNode = rNode;
    
    // build the url to connect
    _url = RplDbusAdapter.buildUrl(_thisRplDbusNode);
    LOG.info("builidng connection to master RPL_DBUS. url=" + _url);
  }
  
  public RplDbusMasterState getMasterState() throws RplDbusException {
    // establish connection
    String url = String.format(_url, _thisRplDbusNode.getUser(), _thisRplDbusNode.getPassword());
    
    try {
      _conn = DriverManager.getConnection(url);
    } catch (SQLException e) {
      String msg = "failed to connect to master RPL_DBUS. Url = " + url;
      LOG.error(msg);
      throw new RplDbusException(msg, e);
    }
    
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try
    {
      stmt = _conn.prepareStatement(RPL_DBUS_STATUS_STMT);
      stmt.executeQuery();
      rs = stmt.getResultSet();
      
      rs.next(); // will throw exception if empty
      RplDbusMasterState rsState = new RplDbusMasterState(rs);
      
      // now figure out server id
      stmt = _conn.prepareStatement(RPL_DBUS_SEVER_ID_STMT);
      stmt.executeQuery();
      rs = stmt.getResultSet();
      
      if(!rs.next())
        throw new SQLException("resultSet is empty");
      rsState.setServerId(rs.getInt(1));
      stmt.close();
      return rsState;
    }
    catch (SQLException e)
    {
      String msg = "cannot execute query on master RPL_DBUS. stmt = " + stmt;
      LOG.error(msg);
      throw new RplDbusException(msg, e);
    } finally {
      if(stmt != null)
        try {
          stmt.close();
        } catch (SQLException e){
          LOG.warn("failed to close stmt");
        }
    }
  }
  public String toString() {
    return _url;
  }
}
