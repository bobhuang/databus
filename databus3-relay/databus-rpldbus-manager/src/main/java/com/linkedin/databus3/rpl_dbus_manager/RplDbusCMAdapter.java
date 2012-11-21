package com.linkedin.databus3.rpl_dbus_manager;


import java.net.InetSocketAddress;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.databus3.cm_utils.ClusterManagerClientAdapter;
import com.linkedin.databus3.cm_utils.ClusterManagerRelayCoordinates;
import com.linkedin.databus3.cm_utils.ClusterManagerResourceKey;
import com.linkedin.databus3.cm_utils.ClusterManagerUtilsException;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.model.ExternalView;


/**
 * An adapter between the client and the RelayClusterManager
 * Provides functionality to instantiate a ClusterManager spectator,
 * listen to external view changes for changes in relays and sources they serve
 *
 */
public class RplDbusCMAdapter extends ClusterManagerClientAdapter
{
  private static Logger LOG = Logger.getLogger(RplDbusCMAdapter.class);

  private RplDbusCMAdapterListener _listener=null;

  /**
   *
   * @param clusterName
   * @param zkConnectString
   * @throws ClusterManagerUtilsException
   * @throws Exception
   */
  public RplDbusCMAdapter(String clusterName, String zkConnectString) throws ClusterManagerUtilsException
  {
    super(clusterName, zkConnectString);
    LOG.info("Creating a rplDbusRelayAdapter (NOT CONNECTED YET) to zkConnectString " + _zkConnectString +
             " on cluster " + _clusterName);
  }

  /**
   * This call actually connects to the ZK and starts listening
   * @param listener
   * @throws ClusterManagerUtilsException
   */
  public void setListener(RplDbusCMAdapterListener listener) throws ClusterManagerUtilsException {
    _listener = listener;
    LOG.info("Setting listener for ZK for RplDubsClusterManager: " + _zkConnectString + "; listener = " + listener);
    connect();
    startListeningForUpdates();
  }

  /**
   * get external most recent SAVED version and convert it into internal format
   * @return map<relay, resources>
   */
  public synchronized Map<ClusterManagerRelayCoordinates, List<ClusterManagerResourceKey>> getCurrentExternalViews()
  {
    //super.getExternalViews(true); // this will populate _externalViews
    if(_externalViews == null)
      return null;

    Map<ClusterManagerRelayCoordinates, List<ClusterManagerResourceKey>> externalViewMap =
        new HashMap<ClusterManagerRelayCoordinates, List<ClusterManagerResourceKey>>();

    // convert into internal format
    for(ZNRecord zn : _externalViews.values()) {
      parseZnRecord(externalViewMap, zn);
    }
    return externalViewMap;
  }


  /**
   * Override the onChange listener
   * locks both helix and cmadapter
   */
  @Override
  public synchronized void onExternalViewChange(List<ExternalView> arg0, NotificationContext arg1) {
      if (arg1 == null)
      {
    	  LOG.error("Got onExternalViewChange notification with null context");
    	  return;
      }

      if ( arg1.getType() == NotificationContext.Type.FINALIZE)
      {
          return;
      }

      // get the current view
      super.onExternalViewChange(arg0, arg1);

      if(_listener != null)
	      _listener.notifyUpdate();

      return;
  }

  /**
   * parse zn record and put it into the view
   * and create map between relay(localhost:11140,ONLINE)=>db_partitions (EspresoDB8,p0_1, EspressoDB8,p2_1...)
   * @param externalView
   * @param zn
   */
  public static void parseZnRecord(
             Map<ClusterManagerRelayCoordinates, List<ClusterManagerResourceKey>> externalView,
             ZNRecord zn) {

    if(zn.getId().equals("relayLeaderStandby")) // skip this
      return;

    boolean debugEnabled = LOG.isDebugEnabled();
    // get all the fileds
    Map<String, Map<String,String>> ev = zn.getMapFields();
    if(debugEnabled)
      LOG.debug("Starting parsing for zn = " + zn);
    for (Map.Entry<String, Map<String, String>> e : ev.entrySet())
    {
      try
      {
        // e.g. "eat1-app13.corp.linkedin.com_20801,ucpx,p0_1,MASTER"
        ClusterManagerResourceKey snRK = new ClusterManagerResourceKey(e.getKey());

        // e.g.
        //  "eat1-app141.corp.linkedin.com_11140" : "ONLINE",
        //  "eat1-app66.corp.linkedin.com_11140" : "ONLINE"
        Map<String, String> relayStates = e.getValue();

        for (Map.Entry<String,String> stateEntry: relayStates.entrySet())
        {
          String relay = stateEntry.getKey(); // relay name and port
          String []ip = relay.split("_");
          if (ip.length != 2)
            throw new ParseException("Relay not specified in the form of IPAddr_PortNo " + relay, ip.length );

          InetSocketAddress ia = new InetSocketAddress(ip[0], Integer.parseInt(ip[1]) );
          ClusterManagerRelayCoordinates relayCoord = new ClusterManagerRelayCoordinates(ia, stateEntry.getValue());
          if(!relayCoord.isOnline()) {
            if(debugEnabled) LOG.debug("ignoring message for relay:" + relayCoord);
            continue;
          }

          // external View maps:
          // "eat1-app66.corp.linkedin.com_11140_ONLINE" =>
          //                     ["eat1-app13.corp.linkedin.com_20801,ucpx,p0_1,MASTER",
          //                     "eat1-app15.corp.linkedin.com_20801,ucpx,p0_1,MASTER"]
          List<ClusterManagerResourceKey> rsKeys = externalView.get(relayCoord);
          if(rsKeys == null) {
            rsKeys = new ArrayList<ClusterManagerResourceKey>(1);
            externalView.put(relayCoord, rsKeys);
          }
          if(snRK.isOnline()) { // we check sn partiton to be SLAVE or MASTER
            rsKeys.add(snRK);
            if(debugEnabled)
              LOG.debug("relay=" + relayCoord + " partition = " + snRK);
          } else  {
            if(debugEnabled)
              LOG.debug("skipping: relay=" + relayCoord + "partition = " + snRK);
          }
        }
      }
      catch(ParseException pe)
      {
        LOG.error("Error parsing a resourceKey : " + e.getKey(), pe);
      }
    }
  }

  /**
   * build string representation of the view
   * @param view
   * @return String representation of the view
   */
  public static String buildString(Map<ClusterManagerRelayCoordinates,List<ClusterManagerResourceKey>> view) {
    StringBuilder sb = new StringBuilder();
    for(Map.Entry<ClusterManagerRelayCoordinates,List<ClusterManagerResourceKey>> e : view.entrySet()) {
      ClusterManagerRelayCoordinates relay = e.getKey();
      List<ClusterManagerResourceKey> rsList = e.getValue();
      sb.append(buildStringOneRelay(relay,rsList));
    }
    sb.append("\n");
    return sb.toString();
  }

  // buiild one relay mapping string
  public static String buildStringOneRelay(ClusterManagerRelayCoordinates relay, List<ClusterManagerResourceKey> rsList) {
    StringBuilder str = new StringBuilder();

    str.append(relay.getAddress() + " => \n");
    int i = 0;
    for(ClusterManagerResourceKey rk : rsList) {
      str.append("\t").append(i++).append(":\t");
      //str.append(rk.getPhysicalPartition() + ";" + rk.getLogicalPartitionNumber());
      str.append(rk);
      str.append("\n");
    }
    str.append("\n");
    return str.toString();
  }
}


