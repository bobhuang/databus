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


public class RplDbusMysqlCoordinatesWithCreds extends RplDbusMysqlCoordinates
{
  private String _user = "";
  private String _password = "";
  
  public String getUser()
  {
    return _user;
  }

  public void setUser(String _user)
  {
    this._user = _user;
  }

  public String getPassword()
  {
    return _password;
  }

  public void setPassword(String _password)
  {
    this._password = _password;
  }

  public RplDbusMysqlCoordinatesWithCreds(String h, int p)
  {
    super(h, p);
  }
  
  public RplDbusMysqlCoordinatesWithCreds(String h, int p, String u, String pw)
  {
    super(h, p);
    _user = u;
    _password = pw;
  }
}
