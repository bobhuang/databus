package com.linkedin.databus3.cm_utils;


import java.net.InetSocketAddress;

public class ClusterManagerRelayCoordinates
{
  /**
   * A user-friendly name to identify the relay
   */
  private final String            _name;

  /**
   * The IP Address and port number
   */
  private final InetSocketAddress _address;

  /**
   * An id for the relay - usually the container id in which relay is running
   */
  private final int               _id;

  /**
   * State of the relay ( as reported by ClusterManager )
   * States are Online, Offline, Error
   */
  private String                  _state;
  
  public ClusterManagerRelayCoordinates(String name, String hostname, int port) {
    this(0, name, new InetSocketAddress(hostname, port));
  }

  /**
   * Base constructor
   * 
   * @param id
   * @param name
   * @param address
   * @param state
   */
  public ClusterManagerRelayCoordinates(int id, String name, InetSocketAddress address, String state)
  {
    _id = id;
    _name = name;
    _address = address;
    _state = state;
  }

  /**
   * Typically called from DatabusHttpV3ClientImpl
   * 
   * @param id
   * @param name
   * @param address
   */
  public ClusterManagerRelayCoordinates(int id, String name, InetSocketAddress address)
  {
    this(id, name, address, "OFFLINE");
  }

  /**
   * Typically called for constructing RelayCoordinates based on Client's external view
   * 
   * @param address
   * @param state
   */
  public ClusterManagerRelayCoordinates(InetSocketAddress address, String state)
  {
    this(0, "default", address, state);
  }

  public String getName() 
  {
    return _name;
  }

  public InetSocketAddress getAddress() 
  {
    return _address;
  }

  public int getId() 
  {
    return _id;
  }

  public void setState(String state)
  {
    if (state.equals("ONLINE") || state.equals("OFFLINE") || state.equals("ERROR"))
      _state = state;
  }
  
  public boolean isOnline() {
    return _state.endsWith("ONLINE");
  }

  public String getState()
  {
    return _state;
  }

  @Override
  public boolean equals(Object o)
  {
    if ( this == o)
      return true;
    if ( o == null)
      return false;
    if (getClass() != o.getClass())
      return false;

    final ClusterManagerRelayCoordinates castedObj = (ClusterManagerRelayCoordinates) o;
    //if ( _address.equals(castedObj.getAddress()) && _state.equals(castedObj.getState()) )
    if(_address.equals(castedObj.getAddress()))
      return true;
    else
      return false;
  }

  @Override
  public int hashCode()
  {
    //return (getAddress().hashCode() << 8) + getState().hashCode();
    return _address.hashCode();
  }
  
  @Override
  public String toString() {
    return "id=" + _id + ";name=" + _name + ";addr=" + _address + ";state=" + _state;
  }
}
