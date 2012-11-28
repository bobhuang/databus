package com.linkedin.databus3.rpl_dbus_manager;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class TestRplDbusMonitor
{
  public static final String MODULE = TestRplDbusMonitor.class.getName();
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
    l.setLevel(Level.INFO);
    Logger.getRootLogger().setLevel(Level.INFO);
    //Logger.getRootLogger().setLevel(Level.DEBUG); 
  }

  
  // can be run only against live instance
  // for example a rpldbus running on port 2900 on localhost 
  // which has two thread connected to ports 3306 and 14100 correspondingly.
  //@Test
  public void testStatus() throws RplDbusException
  {
    //String tmp = System.getenv("MYSQL_SLAVE_JDBC_URL");
    //String url = "jdbc:mysql://localhost:29000/mysql?user=root";
    RplDbusManagerConfigBuilder configBuilder = new RplDbusManagerConfigBuilder();
    RplDbusMysqlCoordinatesWithCreds node = new RplDbusMysqlCoordinatesWithCreds("localhost", 29000, "root", "");
    RplDbusAdapter rplDbusAdapter = new RplDbusAdapter(configBuilder.build(), node);
    RplDbusMysqlCoordinates snMysql1 = new RplDbusMysqlCoordinates("localhost", 3306);
    RplDbusMysqlCoordinates snMysql2 = new RplDbusMysqlCoordinates("localhost", 14100);
    RplDbusMonitor m1 = new RplDbusMonitor(rplDbusAdapter, snMysql1);
    RplDbusMonitor m2 = new RplDbusMonitor(rplDbusAdapter, snMysql2);
    
    LOG.setLevel(Level.INFO);

    try
    {
      LOG.info("m1= " + m1.toStringWithException());
      LOG.info("m2= " + m2.toStringWithException());
    }
    catch (RplDbusException e)
    {
      LOG.error("Error reading RPL DBUS slave status: " + e.getMessage());
    }    
  }
}