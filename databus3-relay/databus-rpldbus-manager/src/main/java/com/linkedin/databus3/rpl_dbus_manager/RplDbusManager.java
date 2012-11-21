package com.linkedin.databus3.rpl_dbus_manager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.monitoring.mbean.StatsCollectors;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.NamedThreadFactory;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.seq.MaxSCNReader;
import com.linkedin.databus2.core.seq.MultiServerSequenceNumberHandler;
import com.linkedin.databus2.core.seq.ServerName;
import com.linkedin.databus3.cm_utils.ClusterManagerRelayCoordinates;
import com.linkedin.databus3.cm_utils.ClusterManagerResourceKey;
import com.linkedin.databus3.cm_utils.ClusterManagerUtilsException;
import com.linkedin.databus3.rpl_dbus_manager.monitoring.mbean.RplDbusTotalStats;


/**
 * @author bshkolni
 *
 * RplDbusManager - manages rpl_dbus instances
 * it creates two adapters:
 *           RplDbusCMAdapter - to receive notifications from Relay ClusterManager
 *           RplDbusAdapter per managed rplDbus instance - to send commands (and get statuses) from RplDbus
 *  NOTE. we create one Manager per rpldbus/relay pair.
 *
 *  ...................describe internal mappings.....................
 *  1.
 *  2. mapping between storageNode coordinates and storage node mysql instances (mysqlinstanceregistry)
 *  3. set of initialized connections - if a thread name not set - make sure to issue a change master command
 *  to this thread
 *  4. storageNode => serverId
 *
 *  ...................flow.................................
 *  RplDbusManager listens for updates from RelayClusterManager's zookeeper.
 *  On each change it gets the copy of the whole ExternalView and trys to consolidate it with
 *  it's own internal state. It compares the mapping it gets from external view and its internal one.
 *  As a result - three actions are possible:
 *    1. EV has new mappings - need to create new Threads, connect and start them
 *    2. InternalMapping has something that is not preset in EV - need to stop this thread
 *    3. sets that match - need to verify they are running
 *  This is done in consolidate() method.
 *
 *  Controling rplDbus
 *  RplDbus is controlled using mysql commands over jdbc connection. Since RpldDbus supports multiple
 *  masters we need a way to specify which master we are issuing a command too. This is done using
 *  'Li_rpl_slave_name' global setting.
 *     mysql> set global Li_rpl_slave_name=THREADNAME
 *     myslq> change master....
 *
 *   the format of THREADNAME is 'host:port' of the storageNode (see {@link RplDbusAdapter}
 *   NOTE. all '.' in the host name are replaced with '_' to satisfy rpldbusmanager slave_name requirement
 *
 *   ... describe commands for getting status and issuing commands ...
 *
 *  WATCHDOG
 *  a separate thread running every minute/5sec/500msec to verify that all the threads that should be RUNINNG are
 *  running and attempts to start them if they are not. In this case it issues stop, reset ,
 *  and change master command. If _needUpdate is set, it also gets the external view and updates internal structure.
 *  At the end of the validationt it reschedules itself. In case of errors next run is in 5sec.
 *  In case of updates in 500ms. Otherwise in a minute.
 *
 */

public class RplDbusManager implements RplDbusCMAdapterListener
{
  public static Logger LOG = Logger.getLogger(RplDbusManager.class.getName());
  final static long RPLDBUS_MANAGER_RETRY_FIX_PERIOD_MSEC=5*1000;
  final static long RPLDBUS_MANAGER_WATCHDOG_PERIOD_MSEC=60*1000;
  final static long RPLDBUS_MANAGER_UPDATE_PERIOD_MSEC=500;

  // mapping between rplDbusofThisRelay and mysql of the StorageNode
  RplDbusMysqlCoordinates _myRplDbusMysql;
  private RplDbusNodeCoordinates _myRelay=null;
  final Map<RplDbusMysqlCoordinates,RplDbusMysqlCoordinates> _mySnViewMysql =
      new HashMap<RplDbusMysqlCoordinates,RplDbusMysqlCoordinates>();

  // storage node => server id
  private final Map<RplDbusMysqlCoordinates, Integer> _storage2ServerId =
      new HashMap<RplDbusMysqlCoordinates, Integer> (10);

  // relay=>sn which had erros
  private final Set<RplDbusMysqlCoordinates> _failedSnUpdates = new HashSet<RplDbusMysqlCoordinates>();

  // number of attempts made to start a thread in error state
  private final Map<String, Integer> _numberOfStartAttemptsForThread = new HashMap<String, Integer>();

  // cluster manager adapter
  private RplDbusCMAdapter _cmAdapter;
  // rplDbus updater
  RplDbusAdapter _myRplDbusAdapter;

  private RplDbusMySqlInstanceRegistry _mysqlInstanceRegistry=null;
  private int _generation = 0;
  private final RplDbusManagerStaticConfig _config;
  private MultiServerSequenceNumberHandler _multiServerSequenceNumberHandler = null;

  Map<String, String> _relayConfigs = null;

  ScheduledExecutorService _scheduler = null;
  // It is possible that notifyUpdate() gets called from different threads, so we mark
  // _updateTaskFuture as volatile
  volatile ScheduledFuture<?> _updateTaskFuture = null;
  UpdateTask _updateTask;
  volatile boolean _needUpdate; // boolean flag that specifies if we need to get new update for the external view

  // stats
  private final StatsCollectors<RplDbusTotalStats> _rplDbusStatsCollector = new StatsCollectors<RplDbusTotalStats>();
  private MBeanServer _mbeanServer;
  public StatsCollectors<RplDbusTotalStats> getRplDbusStatsCollectors() {
    return _rplDbusStatsCollector;
  }
  public void setMBeanServer(MBeanServer server) {
    _mbeanServer = server;
  }

    public RplDbusManager(RplDbusManagerStaticConfig config)
      throws RplDbusException, InvalidConfigException {
    this(config, null);
  }

  /**
   * constructor
   * @param config
   * @param multiServerSequenceNumberHandler
   * @throws RplDbusException
   * @throws InvalidConfigException
   */
  public RplDbusManager(RplDbusManagerStaticConfig config, MultiServerSequenceNumberHandler multiServerSequenceNumberHandler)
      throws RplDbusException, InvalidConfigException {

    if(!config.getEnabled())
      throw new RplDbusException("RplDbusManager is not enabled");

    _config = config;
    _multiServerSequenceNumberHandler = multiServerSequenceNumberHandler;

    // relay's name
    String instance = config.getInstanceName();
    if(instance == null || instance.equals("")) {
      throw new InvalidConfigException("RplDbus instance name is not set");
    }
    _myRelay = new RplDbusNodeCoordinates(config.getInstanceName());

    // TODO - make adapters final
    createRplDbusAdapters(config);
  }

  /**
   * connects to rpldbus and configures tcp port for it
   * @param port
   * @throws RplDbusException
   */
  public void configureRplDbusTcpPort(int port) throws RplDbusException  {
    _myRplDbusAdapter.configureTcpPort(port);
  }

  /**
   * start listening for CM updates and make callbacks
   * @throws RplDbusException
   */
  public void start() throws RplDbusException {
    if(_cmAdapter == null) {
      try {
        _cmAdapter = new RplDbusCMAdapter(_config.getRelayClusterName(), _config.getRelayZkConnectString());
      } catch (ClusterManagerUtilsException e1) {
        throw new RplDbusException("cannot create CMAdapter for cn=" + _config.getRelayClusterName() +
                                   " and zk= " + _config.getRelayZkConnectString(), e1);
      }
    }
    // we need a mapping between SN and its mysql instance
    try {
      _mysqlInstanceRegistry = new RplDbusMySqlInstanceRegistry(_config, _cmAdapter);
    } catch (InvalidConfigException e1) {
      throw new RplDbusException("cannot create mapping between SN and SNMysqlPort", e1);
    }

    // Manager should be able to process notificatios by now, because as soon as we set the listener
    // it starts getting the updates
    try {
      _cmAdapter.setListener(this);
    } catch (ClusterManagerUtilsException e) {
      String msg = "cannot set listener to zk " + _cmAdapter;
      LOG.error(msg, e);
      throw new RplDbusException(msg, e);
    }

    if(_cmAdapter != null && _relayConfigs != null && _relayConfigs.size()>0) {
      //update configs
      _cmAdapter.writeRelayConfigs(_relayConfigs);
    }
  }

  // set adapter created somewhere else (for example a mock one)
  void setCMAdapter(RplDbusCMAdapter adapter) {
    _cmAdapter = adapter;
  }

  //
  // parse configuration and create mapping between Relay and RplDbus instances
  //
  private void createRplDbusAdapters(RplDbusManagerStaticConfig config) throws RplDbusException {
    // get the mapping betweend relays and rpldbuses from the config
    String relayMapping = config.getRelayRplDbusMapping();
    if(relayMapping == null) {
      String error = "Configuration for RplDbusManager is missing RelayDbusMapping";
      LOG.error("error");
      throw new RplDbusException(error);
    }

    // map for RelayClusterManager update
    _relayConfigs = new HashMap<String, String>(1);

    RplDbusNodeCoordinates[] pair;
    try{
      pair = RplDbusNodeCoordinates.parseNodesMapping(relayMapping);
    } catch (InvalidConfigException e) {
      throw new RplDbusException("cannot parse maping " + relayMapping);
    }
    RplDbusNodeCoordinates relay = pair[0];
    RplDbusNodeCoordinates rplDbus = pair[1];
    RplDbusMysqlCoordinatesWithCreds rplDbusWCred =
        new RplDbusMysqlCoordinatesWithCreds(rplDbus.getHostname(), rplDbus.getPort(),
                                             config.getRplDbusMysqlUser(), config.getRplDbusMysqlPassword());

    // adapter to the rpldbus
    RplDbusAdapter rplDbusAdapter = createRplDbusAdapter(config, rplDbusWCred);
    _myRplDbusAdapter = rplDbusAdapter;
    _myRplDbusMysql = rplDbusWCred;

    _relayConfigs.put(relay.mapString(), Integer.toString(rplDbus.getPort()));

    // stop all running threads - they will be started when time comes (with notification)
    LOG.info("Stopping and resetting all rpldbus threads on start");
    rplDbusAdapter.stopAllThreads(true);
  }

  // create new rpldbus adapter
  // is overridden in test class to create mock adapter
  protected RplDbusAdapter createRplDbusAdapter(RplDbusManagerStaticConfig config, RplDbusMysqlCoordinatesWithCreds rplDbusWCred)
      throws RplDbusException {
    return new RplDbusAdapter(config, rplDbusWCred, _rplDbusStatsCollector);
  }

  public RplDbusAdapter getRplDbusAdapter() {
    return _myRplDbusAdapter;
  }

  // TODO - should we use it?
  public void setNewRplDbusAdapter(RplDbusAdapter adapter) {
    _myRplDbusAdapter = adapter;
  }

  // run update of the current state and then validate
  void updateValidateState() {
    long delay = RPLDBUS_MANAGER_WATCHDOG_PERIOD_MSEC; //periodical check
    Map<ClusterManagerRelayCoordinates, List<ClusterManagerResourceKey>> externalView = null;
    // get fresh external view
    try {
      if(_needUpdate) {
        _needUpdate = false; // next time just run validate
        externalView = _cmAdapter.getCurrentExternalViews();
      }

      boolean errors = false;
      synchronized(this) {
        try {
          if(externalView != null) {
            _failedSnUpdates.clear();
            updateRecordsInternal(externalView);
          }

          // validate current state
          validateCurrentState();
        } catch (RplDbusException e) {
          LOG.warn("validateCurrentState failed", e);
          errors = true;
        }
      }

      // we will always use MSEC
      if(errors || _failedSnUpdates.size() != 0) {
        LOG.warn("UpdateTask run status:errors=" + errors  + ",failedUpdates=" + _failedSnUpdates);
        delay = RPLDBUS_MANAGER_RETRY_FIX_PERIOD_MSEC; // error happened - check again
      }
    } finally {
      // reschedule
      scheduleUpdateValidate(delay);
    }
  }

  private void scheduleUpdateValidate(long delay) {
    // start the timer
    if(_scheduler == null) {
      _scheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("ValidatorOrUpdater"));
      _updateTask = new UpdateTask();
    }

    // cancel any current updates - will schedule another one after update
    if(_updateTaskFuture != null) {

      boolean res = _updateTaskFuture.cancel(false);
      LOG.debug("canceling prev update. res="+res);
      _updateTaskFuture = null;
    }

    // schedule task to run soon
    LOG.debug("scheduling new rpldbus update");
    _updateTaskFuture = _scheduler.schedule(_updateTask, delay, TimeUnit.MILLISECONDS);
  }

  @Override
  public void notifyUpdate() {  // this method is synched on _cmAdapter in the caller
    _needUpdate = true;
    scheduleUpdateValidate(RPLDBUS_MANAGER_UPDATE_PERIOD_MSEC);
  }

  /**
   * RplDbusCMAdapterListener interface
   * currently we are NOT using this interface
   */
  @Override
  public synchronized void updateRecords(Map<ClusterManagerRelayCoordinates, List<ClusterManagerResourceKey>> externalView) {
    //updateValidateState(externalView);
    throw new RuntimeException("Shouldn't be used");
  }

  /**
   * update the internal view based on external view
   * externalView = map <relayCoordinate=>resourceKey>
   * for example:
   *      localhost:11140=>"eat1-app11.corp.linkedin.com_12918,BizProfile,p11_1,MASTER"
   */
  public synchronized void updateRecordsInternal(Map<ClusterManagerRelayCoordinates, List<ClusterManagerResourceKey>> externalView) {
    _generation ++; // used to figure out deletion/additions
    boolean thisRelayGotUpdate = false;
    boolean debugEnabled = LOG.isDebugEnabled();

    for(Map.Entry<ClusterManagerRelayCoordinates, List<ClusterManagerResourceKey>> e : externalView.entrySet()) {
      ClusterManagerRelayCoordinates relayCM = e.getKey();
      RplDbusNodeCoordinates relay = new RplDbusNodeCoordinates(relayCM.getAddress().getHostName(),
                                                                relayCM.getAddress().getPort());
      List<ClusterManagerResourceKey> rsList = e.getValue();

      // process only updates relevant to this relay
      if(relay.equals(_myRelay)) {
        LOG.info("updating records for this relay:\n" + RplDbusCMAdapter.buildStringOneRelay(relayCM, rsList));
        consolidateRecords(relay, rsList);
        thisRelayGotUpdate = true;
      } else {
        if(debugEnabled)
          LOG.debug("skipping records with for other relays:\n" + RplDbusCMAdapter.buildStringOneRelay(relayCM, rsList));
      }
    }

    // make sure that this relay got updated - if there are not updates for this relay in the external view
    // it means this relay have been removed - so we need to stop the threads
    if(!thisRelayGotUpdate) {
      LOG.warn("this relay("+_myRelay+") didn't get any updates, if no SNs mapped to it anymore, we are going to stop the threads");
      consolidateRecords(_myRelay, new ArrayList<ClusterManagerResourceKey>());
    }
  }

  // figure out which records stay, gets added or gets removed
  private void consolidateRecords(RplDbusNodeCoordinates ncRelay, List<ClusterManagerResourceKey> externalRsList) {
    _failedSnUpdates.clear();
    boolean debugEnabled = LOG.isDebugEnabled();
    // if something missing from the list - add it
    // if something is one the internal list , but not on external - remove it

    // we use a map - but it is basically a set
    // because it maps storageNode to itself
    if(debugEnabled)
      LOG.debug("current number of SN mappings for " + _myRplDbusMysql + " is " + _mySnViewMysql.size());

    // build a "map/set" of the new storage nodes
    for(ClusterManagerResourceKey rk : externalRsList) {
      String phSource = rk.getPhysicalSource();
      String [] node = phSource.split("_");  // TBD - use parsing from elsewhere...
      if(node.length != 2) {
        LOG.error("invalid key in update" + rk);
        continue; //TBD or throw exception ?????
      }
      RplDbusNodeCoordinates ncStorageNode = new RplDbusNodeCoordinates(node[0], Integer.parseInt(node[1]), _generation);
      RplDbusMysqlCoordinates snMysql;
      try {
        snMysql = _mysqlInstanceRegistry.getMysqlInstance(ncStorageNode);
      }  catch (RplDbusException e) {
        LOG.error("cannot get mysql for the " + phSource, e);
        continue;
      }
      RplDbusMysqlCoordinates snMysqlNew = new RplDbusMysqlCoordinates(snMysql.getHostname(), snMysql.getPort(), _generation);
      // map to itself - if it exists key will be the same, but the value will have different generation
      // so this way we can figure out which one is :
      // 1. new (generations equal), which one is
      // 2. val.generation < _generation - wasn't updated, needs to be removed
      // 3. no change
      _mySnViewMysql.put(snMysqlNew, snMysqlNew);
    }

    // go over all the keys and values in the SET and figure out which one needs to be added and which needs to be removed
    for(Iterator<RplDbusMysqlCoordinates> it = _mySnViewMysql.keySet().iterator(); it.hasNext(); ) {
      RplDbusMysqlCoordinates snMysqlKey = it.next();
      RplDbusMysqlCoordinates val = _mySnViewMysql.get(snMysqlKey); //basically the same object but they may differ in the generation value

      if(debugEnabled)
        LOG.debug("consolidate recs - processing: rpldbus = " + _myRplDbusMysql + " to sn_mysql=" + snMysqlKey);

      try {
        // is not present in the new external view
        if(val.getGen() < _generation) {
          if(slaveThreadCaughtUp(snMysqlKey)) // disable thread only when slave caught up
            removeStorageNode(snMysqlKey);
          it.remove();//removes actual values from the set
          continue;
        }

        // else. If gen of the key is the same as gen of the value, it means it just have been added
        if(val.getGen() == snMysqlKey.getGen()) {
          addStorageNode(snMysqlKey);
          continue;
        }
      }
      catch (RplDbusException e)
      {
        LOG.error("failed to update/validate connection to snMysql=" + snMysqlKey + "thru rpldbus " + _myRplDbusAdapter, e);
        // keep updating others, but record the errors
        _failedSnUpdates.add(snMysqlKey);
      }
    }
  }


  /**
   * validates the rplDbus state against current internal state
   * if(there are some threads in the running state which are not in the internal state - they must be stopped.
   * @param rplDbusMysql
   * @throws RplDbusException
   */
  synchronized void validateCurrentState() throws RplDbusException {
    if(_mySnViewMysql.size() == 0) {
      return; // we don't do any validation until we have our own state
    }
    boolean debugEnabled = LOG.isDebugEnabled();

    // 1. get the current states
    Map<String, RplDbusState> currentMap = new HashMap<String, RplDbusState>(_myRplDbusAdapter.getStates());

    // get SN mysqls we are supposed to be connected to
    Set<RplDbusMysqlCoordinates> snMysqls = _mySnViewMysql.keySet();
    if(debugEnabled)
      LOG.debug("running validateCurrentState. currentMap =" + currentMap.size() + "; myview = " + snMysqls.size());

    // to do reverse lookup we need to keep track of all snMysql names in the currentMap
    Set<RplDbusMysqlCoordinates> mysqlsInCurrentMap = new HashSet<RplDbusMysqlCoordinates>(currentMap.size());

    // go over each one and see if we know about it..
    for(Map.Entry<String, RplDbusState> e : currentMap.entrySet()) {
      String mysqlThreadName = e.getKey();
      RplDbusState realRplDbusState = e.getValue();
      RplDbusMysqlCoordinates realSnMysql =
          new RplDbusMysqlCoordinates(realRplDbusState.getMasterHost(), realRplDbusState.getMasterPort());


      // update stats
      Integer serverId = realRplDbusState.getMasterServerid();
      if(serverId <= 0)
        LOG.warn("Server id is " + serverId + " for " + realSnMysql);
      getStatsObject(serverId).registerState(realRplDbusState);
      if(debugEnabled)
        LOG.debug("state is " + realRplDbusState);

      // special case, for localhost thread name will "locahost:port", but the MasterHost will be FQDN
      // and our mapping will have "localhost"
      if(mysqlThreadName.startsWith("localhost"))
      {
        realSnMysql = new RplDbusMysqlCoordinates("localhost", realRplDbusState.getMasterPort());
      }
      mysqlsInCurrentMap.add(realSnMysql); // keep it for later, for revers lookup

      // If we can get the binlog position of this server, print it out in the status.
      // NOTE. we don't really need it, only as debug/stats
      String binlogPos = "";
      if (serverId != null && serverId.intValue() >= 0)
      {
        long offset = figureOutConnectionFileAndOffset(serverId.intValue(), false);
        if (offset >= 0)
        {
          // We got a valid offset that we can print.
          binlogPos = "(" + RplDbusAdapter.getFileNameIndexFromBinlogOffset(offset)
              + "," + RplDbusAdapter.getFileOffsetFromBinlogOffset(offset) + ")";
        }
      }

      LOG.info("validating: slave_name = " + mysqlThreadName + " state=" + realRplDbusState +
                   "; SNMysql=" + realSnMysql +
                    (binlogPos.isEmpty() ? "" : "; currentPosition=" + binlogPos));
      // see if we have this rplDbus in our mapping
      if(snMysqls.contains(realSnMysql)) {
        if(realRplDbusState.isRplDbusError()) {
          LOG.warn("Thread " + realSnMysql + " has errors: " + realRplDbusState);
        }

        // keep count of how many times we attampted to start this thread
        Integer numAttempts = _numberOfStartAttemptsForThread.get(mysqlThreadName);
        if(numAttempts == null)
          numAttempts = new Integer(0);

        // make sure it is up
        if(!realRplDbusState.isRplDbusUp()) {
          numAttempts ++;
          LOG.warn("Found Thread " + realSnMysql +
                   ". It supposed to be up, but is not. Trying for " + numAttempts + "th time to start it");
          // try to start it
          addStorageNode(realSnMysql);
        } else {
          numAttempts = 0; // reset attempt counter
        }
        _numberOfStartAttemptsForThread.put(mysqlThreadName, numAttempts);
      } else {
        // thread which is not under RPLDBUSmanager control - needs to be STOPPED
        // but before we stop them we need to make sure all the data from that master has been processed
        LOG.warn("Found unmanaged rplDbus thread " + realSnMysql + ". Checking it out");
        if(realRplDbusState.getSlaveIOThreadRunning() || realRplDbusState.getSlaveSQLThreadRunning()) {
          LOG.warn("Unmanaged rplDbus thread " + realSnMysql + " is running. Stopping it");
          if(slaveThreadCaughtUp(realSnMysql))
            removeStorageNode(realSnMysql);
        }
      }
    }

    // now the other direction
    for(RplDbusMysqlCoordinates snMysql : snMysqls) {
      if(!mysqlsInCurrentMap.contains(snMysql)) {
        // need to be added
        addStorageNode(snMysql);
      }
    }
    if(snMysqls.size() > currentMap.size()) {
      // very unlikely (DDSDBUS-662)
      // some threads are not even CREATED. TODO - more detailed diagnostics and may be even recovery
      LOG.error("some threads are not CREATED." + snMysqls.size() + " > " + currentMap.size());
    }
  }

  // disconnect the rpl of this relay this storageNode
  private void removeStorageNode(RplDbusMysqlCoordinates snMysqlToRemove)
      throws RplDbusException {
    LOG.info("Issuing - removing connection from  relay = " + _myRplDbusMysql + " to sn " + snMysqlToRemove);
    _myRplDbusAdapter.disconnectFromNode(snMysqlToRemove);
  }

  //
  /**
   *  connect the rpl of this relay to this storageNode
   * @param snMysqlMasterToAdd
   * @param state (optional), pass null
   * @throws RplDbusException
   */
  private void addStorageNode(RplDbusMysqlCoordinates snMysqlMasterToAdd)
      throws RplDbusException {
    LOG.info("About to connect relay = " + _myRplDbusMysql + " to snMaster " + snMysqlMasterToAdd);

    // get Master server id
    Integer serverIdInt = _storage2ServerId.get(snMysqlMasterToAdd);
    if(serverIdInt == null) {
      RplDbusMasterAdapter masterAdapter = _myRplDbusAdapter.createRplDbusMasterAdapter(snMysqlMasterToAdd);
      RplDbusMasterState masterState = masterAdapter.getMasterState();

      if(masterState != null) {
        int serverId = masterState.getServerId();
        if(serverId>0) {
          serverIdInt = new Integer(serverId);
          _storage2ServerId.put(snMysqlMasterToAdd, serverIdInt);
        }
      }
    }
    LOG.debug("Got master Serverid = " + serverIdInt);

    if(serverIdInt.intValue()<0) {
      LOG.warn("cannot get server id for mysql " + snMysqlMasterToAdd);
    }

    // figure out the offset for the serverId
    long offset = figureOutConnectionFileAndOffset(serverIdInt, true);

    //connect this this node
    _myRplDbusAdapter.connectToNode(snMysqlMasterToAdd, offset);
    getStatsObject(serverIdInt).registerChangeMasterCalled();
    getStatsObject(serverIdInt).registerStartTime(System.currentTimeMillis());
  }

  private RplDbusTotalStats getStatsObject(Integer serverId) {
    String serverIdStr = "NONE"; // fake id. Guaranteed we get an object back and keep track on erroneous calls
    if(serverId != null)
      serverIdStr = serverId.toString();

    RplDbusTotalStats stats = _rplDbusStatsCollector.getStatsCollector(serverIdStr);
    if(stats == null) {
      stats = new RplDbusTotalStats(this.hashCode(), "rpldbus-" + serverIdStr, true, true, null);
      _rplDbusStatsCollector.addStatsCollector(serverIdStr, stats);

      if(_mbeanServer != null)
        stats.registerAsMbean(_mbeanServer);
    }
    return stats;
  }


  // returns -1 if cannot read offset for the serverid
  public long figureOutConnectionFileAndOffset(final int serverId, boolean create) {
    long offset = -1;
    if(_multiServerSequenceNumberHandler == null)
      return offset;

    boolean debugEnabled = LOG.isDebugEnabled();

    ServerName namedServerId = new ServerName(serverId);
    MaxSCNReader maxSCNReader = null;
    if (create)
    {
      try
      {
        maxSCNReader = _multiServerSequenceNumberHandler.getOrCreateHandler(namedServerId);
      }
      catch(DatabusException e)
      {
        LOG.error("Could not get SCN reader for server id " + serverId, e);
      }
    }
    else
    {
      maxSCNReader = _multiServerSequenceNumberHandler.getHandler(namedServerId);
    }
    if (maxSCNReader == null) {
      LOG.error("Couldn't get SCN reader for server id " + serverId);
      return offset;
    }
    try
    {
      offset = maxSCNReader.getMaxScn();
      if(debugEnabled)
        LOG.debug("parsed offset " + offset +
                  ";file_index =" + RplDbusAdapter.getFileNameIndexFromBinlogOffset(offset) +
                ";offset_in_file=" + RplDbusAdapter.getFileOffsetFromBinlogOffset(offset));
    }
    catch (DatabusException e)
    {
       LOG.error("couldn't get the offset for server id " + serverId);
    }

    return offset;
  }

  /**
   * checks if SQL thread caught up with IO thread
   * @param snMysqlMasterToAdd - storage node mysql (master)
   * @return true if slave completely caught up (in case of master not available - check only slave threads)
   * @throws RplDbusException
   */
  public boolean slaveThreadCaughtUp(RplDbusMysqlCoordinates rplDbusMysql) throws RplDbusException {
    //RplDbusAdapter rplDbusAdapter = getAdapterForThisRplDbus();
    RplDbusState slaveState = _myRplDbusAdapter.getState(rplDbusMysql);

    // first see if we can connect to the master
    RplDbusMasterAdapter masterAdapter =  _myRplDbusAdapter.createRplDbusMasterAdapter(rplDbusMysql);
    RplDbusMasterState masterState = null;
    try {
      masterState = masterAdapter.getMasterState();
    } catch (RplDbusException e) {
      LOG.warn("cannot get master state for " + rplDbusMysql, e);
    }

    // 1. Exec_Master_Log_Pos == Position (from SHOW MASTER STATUS on the master) and
    //    Relay_Master_Log_File == File (from SHOW MASTER STATUS on the master)
    if(masterState != null &&
        (slaveState.getExecMasterLogPos() != masterState.getPosition() ||
        !slaveState.getMasterLogFile().equals(masterState.getFile()))) {
      LOG.warn("checking for slave IO thread catching up with master failed: slaveState=" + slaveState + ";masterState=" +  masterState);
      return false;
    }

    // 2. If Exec_Master_Log_Pos == Read_Master_Log_Pos and
    // Master_Log_File == Relay_Master_Log_File (all from SHOW SLAVE STATUS on RPL_DBUS)
    // the SQL thread on RPL_DBUS has caught up to the I/O thread.
    if(slaveState.getExecMasterLogPos() != slaveState.getMasterLogPos() ||
        ! slaveState.getMasterLogFile().equals(slaveState.getRelayMasterLogFile())) {
      LOG.warn("checking for slave SQL thread catching up with IO thread failed: slaveState=" + slaveState);
      return false;
    }

    return true;
  }


  // string representation of the view
  private String buildViewString() {
    StringBuilder sb = new StringBuilder();
    sb.append(_myRplDbusMysql).append(" => \n");
    //for(Map.Entry<RplDbusMysqlCoordinates, Map<RplDbusMysqlCoordinates, RplDbusMysqlCoordinates>> e : _myViewMysql.entrySet()) {
    for(RplDbusMysqlCoordinates snMysql : _mySnViewMysql.keySet()) {
        sb.append("\t").append(snMysql).append("\n");
    }
    sb.append("\n");
    return sb.toString();
  }

  @Override
  public String toString() {
    return buildViewString();
  }

  /**
   * restart thread identified by serverId and reset it to the offset
   * @param serverId
   * @param offset
   * @throws RplDbusException
   */
  public void restartThread(int serverId, long offset) throws RplDbusException {
    // figure out storage node for the serverId
    RplDbusMysqlCoordinates snMysql = null;
    LOG.info("restarting rpldbus thread for sid:" + serverId + "; offset=" + offset);
    for(Map.Entry<RplDbusMysqlCoordinates, Integer> e : _storage2ServerId.entrySet()) {
      int sid = e.getValue();
      if(sid == serverId) {
        snMysql = e.getKey();
        break;
      }
    }
    if(snMysql == null) {
      throw new RplDbusException("canot find thread for server id " + serverId);
    }

    _myRplDbusAdapter.restartThread(snMysql, offset);
  }

  /**
   * @param args
   */
  public static void main(String[] args)
  {
  }

  //
  final class UpdateTask implements Runnable {
    UpdateTask() {
    }
    @Override
    public void run() {
      Thread.currentThread().setName("UpdateValidateTask");
      LOG.debug("Starting updateValidate task");
      updateValidateState();
    }
  }

  /**
   * shutdowns all the timers and scheduler
   */
  public void shutdown() {
    if(_updateTaskFuture != null)
      _updateTaskFuture.cancel(false);
    if(_scheduler != null)
      _scheduler.shutdown();
    if (_cmAdapter != null)
	  _cmAdapter.disconnect();

  }
}
