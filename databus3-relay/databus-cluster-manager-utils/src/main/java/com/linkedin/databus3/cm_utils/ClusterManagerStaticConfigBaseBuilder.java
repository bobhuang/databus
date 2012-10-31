package com.linkedin.databus3.cm_utils;

import java.net.UnknownHostException;

import org.apache.log4j.Logger;

public class ClusterManagerStaticConfigBaseBuilder {
	  protected boolean _enabled = false;
	  protected boolean _enableDynamic = false;
	  protected String _version = null;
	  protected String _relayZkConnectString;
	  protected String _relayClusterName;
	  protected String _instanceName;
	  protected String _fileName;
	  public final static String USE_LOCAL_HOST_NAME = "useLocalHostName";
	  private static Logger LOG = Logger.getLogger(ClusterManagerStaticConfigBaseBuilder.class);
	  
	  /**
	   * substitute this property with real hostname for this host
	   * @param hostname
	   * @return fqdn of the hostname
	   */
	  public static String convertLocalHostName(String hostname) {
	    String[] parts = hostname.split("_");
      if (parts.length == 2 && parts[0].equals(USE_LOCAL_HOST_NAME))
      {
        String portNum = parts[1];
        try
        {
          hostname = java.net.InetAddress.getLocalHost().getCanonicalHostName();
          hostname = hostname + "_" + portNum;
          LOG.info("Modified instanceName to have a canonical hostname as " + hostname);
        }
        catch (UnknownHostException e)
        {
          hostname = "localhost" + "_" + portNum;
          LOG.info("Modified instanceName to have a canonical hostname as " + hostname);
        }
      }
      return hostname;
	  }
	  
	  public ClusterManagerStaticConfigBaseBuilder()
	  {
		  _enabled = false;		  
	  }
	  
	  public boolean getEnabled() {
		return _enabled;
	  }

	  public void setEnabled(boolean enabled) {
		_enabled = enabled;
	  }

	  public String getVersion() {
		return _version;
	  }

	  public void setVersion(String version) {
		_version = version;
	  }

	  public String getRelayZkConnectString() {
		  return _relayZkConnectString;
	  }

	  public void setRelayZkConnectString(String relayZkConnectString) {
		  _relayZkConnectString = relayZkConnectString;
	  }

	  public String getRelayClusterName() {
		  return _relayClusterName;
	  }

	  public void setRelayClusterName(String relayClusterName) {
		  _relayClusterName = relayClusterName;
	  }

	  public String getInstanceName() {
		  return _instanceName;
	  }

	  public void setInstanceName(String instanceName) {
		  _instanceName = convertLocalHostName(instanceName);
	  }  
	  public String getFileName() {
		  return _fileName;
	  }

	  public void setFileName(String fileName) {
		  _fileName = fileName;
	  }

	
	  public boolean getEnableDynamic() {
		return _enableDynamic;	
	  }
	
	  public void setEnableDynamic(boolean enableDynamic) {
		this._enableDynamic = enableDynamic;	
	  }  
	  
	  
}
