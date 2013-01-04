package com.linkedin.databus3.rpl_dbus_manager;


/**
 * Static configuration for rplDbus
 */
public class RplDbusManagerStaticConfig
{
  private boolean _enabled = false;
  private String _version = null;
  private String _relayZkConnectString;
  private String _relayClusterName;
  private String _instanceName = null;
  private String _relayRplDbusMapping = null;
  private String _mysqlPortMapping = null;
  
  private String _rplDbusUser = "";
  private String _rplDbusPassword = "";
  
  // we need to figure out the correct way of getting passwords - for now same for all SNs
  private String _snUser = "root";
  private String _snPassword = "";
  
  
  public String getMysqlPortMapping() {
    return _mysqlPortMapping;
  }
  
  public String getRplDbusMysqlUser()
  {
    return _rplDbusUser;
  }

  public String getRplDbusMysqlPassword()
  {
    return _rplDbusPassword;
  }
  
  public String getStorageNodeMysqlUser()
  {
    return _snUser;
  }

  public String getStorageNodeMysqlPassword()
  {
    return _snPassword;
  }


  /**
   * 
   * @param enabled
   * @param version - currently not used
   * @param relayZkConnectString
   * @param relayClusterName
   * @param thisRelay ("host:port")
   */
  public RplDbusManagerStaticConfig(boolean enabled, String version, String relayZkConnectString, 
                                    String relayClusterName, String instanceName, String relayMapping,
                                    String mysqlPortMapping, String rplDbusMysqlUser, String rplDbusMysqlPassword,
                                    String storageNodeMysqlUser, String storageNodeMysqlPassword)
  {
    super();
    _enabled = enabled;
    _version = version;
    _relayZkConnectString = relayZkConnectString;
    _relayClusterName = relayClusterName;
    _instanceName = instanceName;
    _relayRplDbusMapping = relayMapping;
    _mysqlPortMapping = mysqlPortMapping;
    _rplDbusPassword = rplDbusMysqlPassword;
    _rplDbusUser = rplDbusMysqlUser;
    _snUser = storageNodeMysqlUser;
    _snPassword = storageNodeMysqlPassword;
  }
  
  public String getRelayRplDbusMapping() {
    return _relayRplDbusMapping;
  }
  
  public String getInstanceName()
  {
    return _instanceName;
  }

  public void setInstanceName(String instanceName)
  {
    _instanceName = instanceName;
  }

  
  public boolean getEnabled()
  {
    return _enabled;
  }
  
  public String getVersion()
  {
    return _version;
  }
  
  public String getRelayZkConnectString()
  {
    return _relayZkConnectString;
  }

  public String getRelayClusterName()
  {
    return _relayClusterName;
  }   
}
