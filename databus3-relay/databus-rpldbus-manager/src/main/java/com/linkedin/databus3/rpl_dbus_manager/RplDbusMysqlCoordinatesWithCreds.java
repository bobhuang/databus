package com.linkedin.databus3.rpl_dbus_manager;

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
