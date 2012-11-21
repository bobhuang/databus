package com.linkedin.databus3.rpl_dbus_manager;

public class RplDbusException extends Exception
{

  public RplDbusException(String msg, Exception e) {
    super(msg, e);
  }
  
  public RplDbusException(String msg) {
    super(msg);
  }
  
  /**
   * 
   */
  private static final long serialVersionUID = 100L;

}
