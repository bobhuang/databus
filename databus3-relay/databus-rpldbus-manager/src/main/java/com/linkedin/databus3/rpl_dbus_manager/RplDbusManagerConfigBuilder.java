package com.linkedin.databus3.rpl_dbus_manager;

import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus3.cm_utils.ClusterManagerStaticConfigBaseBuilder;



public class RplDbusManagerConfigBuilder  extends ClusterManagerStaticConfigBaseBuilder
implements ConfigBuilder<RplDbusManagerStaticConfig>{
  protected boolean _enabled = false;
  protected String _version = null;
  protected String _relayRplDbusMapping;
  protected String _mysqlPortMapping;
  
  // passwords
  // used to connect to mysql
  protected String _rplDbusMysqlUser;
  protected String _rplDbusMysqlPassword;
  
  // used to connect rplDbus to mysql on storage node
  protected String _storageNodeMysqlUser;
  protected String _storageNodeMysqlPassword;

  public RplDbusManagerConfigBuilder() {
    _enabled = false;
    _rplDbusMysqlUser = "root";
    _rplDbusMysqlPassword = "";

    _storageNodeMysqlUser = "rplespresso";
    _storageNodeMysqlPassword = "espresso";
  }
  
  public String getRplDbusMysqlUser()  {
    return _rplDbusMysqlUser;
  }

  public void setRplDbusMysqlUser(String rplDbusMysqlUser) {
    this._rplDbusMysqlUser = rplDbusMysqlUser;
  }

  public String getRplDbusMysqlPassword() {
    return _rplDbusMysqlPassword;
  }

  public void setRplDbusMysqlPassword(String rplDbusMysqlPassword) {
    this._rplDbusMysqlPassword = rplDbusMysqlPassword;
  }

  public String getStorageNodeMysqlUser() {
    return _storageNodeMysqlUser;
  }

  public void setStorageNodeMysqlUser(String storageNodeMysqlUser) {
    this._storageNodeMysqlUser = storageNodeMysqlUser;
  }

  public String getStorageNodeMysqlPassword() {
    return _storageNodeMysqlPassword;
  }

  public void setStorageNodeMysqlPassword(String storageNodeMysqlPassword) {
    this._storageNodeMysqlPassword = storageNodeMysqlPassword;
  }

  
  public String getmysqlPortMapping() {
    return _mysqlPortMapping;
  }
  public void setMysqlPortMapping(String mapping) {
    _mysqlPortMapping = mapping;
  }

  public String getRelayRplDbusMapping() {
    return _relayRplDbusMapping;
  }

  /**
   * mapping between relay and its rpldbus
   * format: relayHost_relayPort:rplDbusHost_relayPort
   * 
   * @param mapping to rplDbus instances
   * @throws InvalidConfigException
   */
  public void setRelayRplDbusMapping(String mapping) throws InvalidConfigException {
    String [] mappings = mapping.split(":");
    if(mappings.length != 2) {
      throw new InvalidConfigException("invalid mapping for relayRplDbusMapping: " + mapping);
    }
    String relay = convertLocalHostName(mappings[0]);
    String rplDbus = convertLocalHostName(mappings[1]);
    this._relayRplDbusMapping = convertLocalHostName(relay + ":" + rplDbus);
  }

  public boolean getEnabled() {
    return _enabled;
  }

  public void setEnabled(boolean enabled) {
    _enabled = enabled;
  }

  @Override
  public RplDbusManagerStaticConfig build()
  {
    return new RplDbusManagerStaticConfig(_enabled, _version, _relayZkConnectString,
                                          _relayClusterName, _instanceName, _relayRplDbusMapping,
                                          _mysqlPortMapping, _rplDbusMysqlUser, _rplDbusMysqlPassword,
                                          _storageNodeMysqlUser, _storageNodeMysqlPassword);
  }

}
