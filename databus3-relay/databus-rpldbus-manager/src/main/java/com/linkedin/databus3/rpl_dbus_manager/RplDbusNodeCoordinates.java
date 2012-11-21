package com.linkedin.databus3.rpl_dbus_manager;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.codehaus.jackson.annotate.JsonProperty;

import com.linkedin.databus.core.util.InvalidConfigException;

public class RplDbusNodeCoordinates
{
  public static final String HOSTNMAME_PORT_SEPARATOR = "_";
  public static final String HOST_MAPPING_SEPARATOR = ":";
  
  protected String _hostname;
  protected int _port;
  protected int _gen; // generations, not part of hashCode or equals
  
  public int getGen()
  {
    return _gen;
  }

  public void setGen(int _gen)
  {
    this._gen = _gen;
  }

  @Override
  public int hashCode()
  {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((_hostname == null) ? 0 : _hostname.hashCode());
    result = prime * result + _port;
    return result;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    RplDbusNodeCoordinates other = (RplDbusNodeCoordinates) obj;
    if (_hostname == null)
    {
      if (other._hostname != null)
        return false;
    }
    else if (!_hostname.equals(other._hostname))
      return false;
    if (_port != other._port)
      return false;
    return true;
  }

  public RplDbusNodeCoordinates(String h, int p, int gen) {
    _hostname = h;
    _port = p;
    _gen = gen;
  }
  
  public RplDbusNodeCoordinates(String h, int p) {
    this(h, p, -1);
  }
  
  // string in format hostname_port
  public RplDbusNodeCoordinates(String hostname_port) throws InvalidConfigException {
    String [] pair = hostname_port.split(HOSTNMAME_PORT_SEPARATOR);
    if(pair.length < 2)
      throw new InvalidConfigException("Ivanlid parameter for RplDbusNodeCoordinates " + hostname_port);
    
    _hostname = pair[0];
    _port = Integer.parseInt(pair[1]);
    _gen = -1;
  }
  
  @JsonProperty
  public String getHostname()
  {
    return _hostname;
  }
  
  /**
   * if hostname is localhost - return a FQDN for it
   * @param hostname
   * @return FQDN 
   */
  public static String convertLocalHostToFqdn(String hostname) {
    
    if(hostname.equals("localhost")) {
      try {
        hostname = InetAddress.getLocalHost().getCanonicalHostName();
      } catch (UnknownHostException e) { } // ignore
    }
    return hostname;
  }

  /*
  private void setHostname(String hostname)
  {
    if(hostname.equals("localhost") || hostname.equals("127.0.0.1")) {
      try {
        _hostname = InetAddress.getLocalHost().getCanonicalHostName();
      } catch (UnknownHostException e) { } // ignore
      return;
    }
    
    InetSocketAddress addr = new InetSocketAddress(hostname, 1000);
    this._hostname = addr.getHostName();
  }
  */
  
  public int getPort()
  {
    return _port;
  }
  public void setPort(int port)
  {
    this._port = port;
  }
  
  @Override
  public String toString() {
    return "[" + _hostname + "_" + _port + "(" + _gen + ")]";
  }
  
  public String mapString() {
    return _hostname + HOSTNMAME_PORT_SEPARATOR + _port;
  }
  
  /**
   * parse two hosts mapping host_port:host1_port1
   * @param mapping
   * @return 2 element array arr[0] mapped to arr[1]
   * @throws InvalidConfigException 
   */
  public static RplDbusNodeCoordinates [] parseNodesMapping(String mapping) throws InvalidConfigException {
    String [] arr = mapping.split(HOST_MAPPING_SEPARATOR);
    if(arr.length != 2) 
      throw new InvalidConfigException("cannot parse host mapping " + mapping);
    
    RplDbusNodeCoordinates [] nodes = new RplDbusNodeCoordinates[2];
    nodes[0] = new RplDbusNodeCoordinates(arr[0]);
    nodes[1] = new RplDbusNodeCoordinates(arr[1]);
    return nodes;
  }
  
  /**
   * 
   * @param from
   * @param to
   * @return returns string in the form 'fromHostname_fromPort:toHostname_toPort'
   */
  public static String createMappingLine (RplDbusNodeCoordinates from , RplDbusNodeCoordinates to) {
    return from.mapString() + HOST_MAPPING_SEPARATOR + to.mapString();
  }
}
