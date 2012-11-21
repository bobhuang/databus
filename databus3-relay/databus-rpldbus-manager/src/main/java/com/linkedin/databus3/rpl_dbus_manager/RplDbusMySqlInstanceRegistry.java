package com.linkedin.databus3.rpl_dbus_manager;

import java.util.HashMap;
import java.util.Map;

import com.linkedin.databus.core.util.InvalidConfigException;


public class RplDbusMySqlInstanceRegistry
{
  // mysql instances for StorageNodes
  private final Map<RplDbusNodeCoordinates, RplDbusMysqlCoordinates> _sn2mysqlInstance;
  private RplDbusCMAdapter _cmAdapter;
  private boolean _modeConfig = false; // if true - using properties file for mapping
  
  public RplDbusMySqlInstanceRegistry(RplDbusManagerStaticConfig config, RplDbusCMAdapter cmAdapter)
      throws InvalidConfigException {
    
    _cmAdapter = cmAdapter;
    _sn2mysqlInstance = new HashMap<RplDbusNodeCoordinates, RplDbusMysqlCoordinates>();
    
    //***********************
    // see if config is available for the mapping
    String mappingConfig = null;
    if(config != null)
      mappingConfig = config.getMysqlPortMapping();
    
    // no way to get the mapping
    if(cmAdapter == null && (mappingConfig == null || mappingConfig.isEmpty())) {
      throw new InvalidConfigException("config and cm adapter both are null/empty");
    }
   
    //config maping
    if(mappingConfig != null && !mappingConfig.isEmpty()) {
      _modeConfig = true;
      String [] mappings = mappingConfig.split(",");
      for(String mapping : mappings) {
        //String [] strs = mapping.split(RplDbusNodeCoordinates.HOST_MAPPING_SEPARATOR);
        RplDbusNodeCoordinates [] nodes = RplDbusNodeCoordinates.parseNodesMapping(mapping);
        //
        // key for the mapping is hostname_port for the storage node
        // NOTE - this name MUST match the name as it appears in the CM external view
        
        RplDbusMysqlCoordinates rplDbusMysql = new RplDbusMysqlCoordinates(nodes[1].getHostname(), nodes[1].getPort());

        RplDbusManager.LOG.debug("MysqlMapping: adding " + nodes[0] + " to " + rplDbusMysql);
        // key is a string in form of host_port
        _sn2mysqlInstance.put(nodes[0], rplDbusMysql);
      }
    }
    // is it possible we left the mapping in the config file by mistake?
    if(cmAdapter != null && _modeConfig)
      RplDbusManager.LOG.warn("Using property based mysql port mapping. CmAdapter is not null.");
  }
  
  /**
   * get mysql coordinates for the storage node
   * @param sn - storage node
   * @return mysql coordinates, or null if not found
   * @throws RplDbusException
   */
  public RplDbusMysqlCoordinates getMysqlInstance(RplDbusNodeCoordinates sn) throws RplDbusException {
    RplDbusMysqlCoordinates mysql = _sn2mysqlInstance.get(sn);
    
    if(mysql != null)
      return mysql;
    
    if(_modeConfig) {
      throw new RplDbusException("Cannot get mysql port for storage node " + sn);
    }
    // try to get it from the cluster
    String port = _cmAdapter.getClusterConfig(sn.mapString());
    RplDbusManager.LOG.info("Getting SN mysql port for the first time for " + sn + " port = " + port);
    
    mysql = new RplDbusMysqlCoordinates(sn.getHostname(), Integer.parseInt(port));
    _sn2mysqlInstance.put(sn, mysql);
    
    return mysql;
  }
}
