package com.linkedin.databus3.espresso.cmclient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.linkedin.databus.core.cmclient.NotAReplicatingResourceException;
import com.linkedin.databus3.espresso.cmclient.Constants;
import com.linkedin.helix.HelixProperty.HelixPropertyAttribute;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.IdealState.IdealStateModeProperty;

public class RelayIdealStateGenerator implements StorageExternalViewChangeObserver
{
	private final String MODULE = RelayIdealStateGenerator.class.getName();
	private final Logger LOG = Logger.getLogger(MODULE);

	private String _storageZkConnectString;
	private String _storageClusterName;
	private String _relayZkConnectString;
	private String _relayClusterName;
	private StorageAdapter _storageAdapter;
	private RelaySpectator _relaySpectator;
	private Set<String> _dbNames;
	private int _replicationFactor;
	private ResourceManager _rm;

	// For testing purposes
	protected RelayIdealStateGenerator()
	{
		 _dbNames                = new HashSet<String>();
		 _rm = null;
	}

	public RelayIdealStateGenerator(int replicationFactor,
                                    String storageZkConnectString, String storageClusterName,
                                    String relayZkConnectString, String relayClusterName, ResourceManager rm)
	{
		 LOG.info("Storage Cluster co-ordinates are :" + storageZkConnectString + " " + storageClusterName);
		 LOG.info("Relay Cluster co-ordinates are :" + relayZkConnectString + " " + relayClusterName);
		 _storageZkConnectString = storageZkConnectString;
		 _storageClusterName     = storageClusterName;
		 _relayZkConnectString   = relayZkConnectString;
		 _relayClusterName       = relayClusterName;
		 _replicationFactor      = replicationFactor;
		 _dbNames                = new HashSet<String>();
		 _rm = rm;
	}

	/**
	 * 1. Connects to the storage cluster
	 * 2. If configured to get updates from external view from storage cluster, configures itself for it
	 *
	 * @throws Exception
	 */
	public void connect(boolean enableUpdates)
	throws Exception
	{
		/*
		 * First connect the relay spectator, so that it is available immediately after an external view
		 * change notification from storage cluster
		 */
		if (null == _relaySpectator )
		{
		  _relaySpectator = new RelaySpectator(_relayZkConnectString, _relayClusterName);
		  _relaySpectator.connect();
		}

		if ( null == _storageAdapter)
		{
			_storageAdapter = new StorageAdapter(_storageClusterName, _storageZkConnectString);
			_storageAdapter.connect();
			if (enableUpdates)
			{
				_storageAdapter.startListeningForUpdates();
				_storageAdapter.addExternalViewChangeObservers(this);
			}
			/*
			 * List of databases in the storage cluster for which Ideal State has to be written
			 */
			_dbNames = new HashSet<String>(_storageAdapter.getDbNames());
		}

	}

	public void disconnect()
	{
		/**
		 * Remove storage
		 */
		disconnectStorageAdapter();
		disconnectRelayAdapter();
		_dbNames.clear();
	}

	public void disconnectStorageAdapter()
	{
		if ( null != _storageAdapter)
		{
			_storageAdapter.disconnect();
			_storageAdapter = null;
		}
		else
		{
			LOG.info("_storageAdapter is null already");
		}
	}

	public void disconnectRelayAdapter()
	{
		if (null != _relaySpectator)
		{
			_relaySpectator.disconnect();
			_relaySpectator = null;
		}
		else
		{
			LOG.info("_storageAdapter is null already");
		}
	}

	public void writeIdealStatesForCluster()
	{
		/**
		 * Writing to ideal state may throw an exception due to various errors that may happen
		 * network failure
		 * contention at cluster manager, etc.
		 * Have a finite number of RETRIES for handle this case
		 */
		final int NUM_RETRIES = 3;

		for (String dbName : _dbNames)
		{
			boolean success = false;
			for (int count=0; (count<NUM_RETRIES) && (success==false); count++)
			{
				try
				{
					/**
					 * Obtain Relay Ideal State and write it to the relay cluster
					 **/
					LOG.info("Calculating final ideal state - by joining initial ideal state with s.ext view");

					IdealState ris = calculateFinalIdealState(dbName);
					writeIdealStateToClusterManager(dbName, ris);

					success = true;
					LOG.info("Succeeded writing ideal state to cluster. Exit loop");
				} catch (NotAReplicatingResourceException re) {
					LOG.info("dbName " + dbName + " follows a state model other than MasterSlave, hence ignoring");
					success = true;
				} catch (Exception e){
					LOG.error("Error writing ideal state of cluster" + e.getMessage());
				}
			}
		}
		return;
	}


	/**
	 *
	 * Calculates ideal state for a given
	 * 	(1) Database
	 *  (2) Replication factor
	 *
	 * @param storageZkConnectString
	 * @param storageClusterName
	 * @param relayZkConnectString
	 * @param relayClusterName
	 * @param dbName
	 * @param relayReplicationFactor
	 * @return
	 * @throws Exception
	 */
	public IdealState calculateFinalIdealState(String dbName)
	throws Exception
	{
		final List<String> relayNodes = _relaySpectator.getInstances();
		return calculateFinalIdealState(dbName, relayNodes);
	}

	/**
	 * Invoked internally to update relay ideal state when
	 * @param ev
	 * @throws Exception
	 */
	synchronized protected void updateIdealState(String dbName, ExternalView evInit)
	throws Exception
	{
		LOG.info("Updating ideal state due to an external view change on storage node");
		if ( null == _relaySpectator)
		{
			LOG.error("_relaySpectator is null");
			return;
		}
		final List<String> relayNodes = _relaySpectator.getInstances();
		try
		{
			final List<String> storageNodes = _storageAdapter.getStorageNodes();
            ZNRecord ev = computeFuncISandEVofStorageCluster(evInit.getRecord(), dbName);

			ZNRecordSerializer zs = new ZNRecordSerializer();
			if (LOG.isDebugEnabled()) {
				LOG.debug("The external view for storage node is :" + new String(zs.serialize(ev)));
			}

			Map<String, Map<String, String>> cis = StorageAdapter.getCalculatedIdealState(relayNodes, storageNodes, _replicationFactor);
			if (LOG.isDebugEnabled()) {
				LOG.debug("The calculated ideal state is " + cis);
			}

			IdealState is = performJoin(cis, ev);

			writeIdealStateToClusterManager(dbName, is);
			if (LOG.isDebugEnabled())
			{
				LOG.debug("Join of calculated ideal state and external view from storage node ");
				LOG.debug(new String(zs.serialize(is.getRecord())));
			}
		}
		catch (NotAReplicatingResourceException e)
		{
			LOG.error("Error while trying to calculate final ideal state for " + e.getResourceName(), e);
		}
		catch (Exception e)
		{
			LOG.error("Error while trying to calculate final ideal state ", e);
		}
		return;
	}

   /**
   *
   * Calculates ideal state for a given pair of
   * Storage cluster, Relay Cluster and a given database, replication factor for relays and list of relays
   *
   * This method takes in a list of nodes directly to be amenable for unit-testing ( as opposed to getting list of relay nodes
   * from the cluster itself )
   *
   * This method does the following :
   * Generates an initial relay ideal state -  dbSource -> relay assigment
   * Looks up external view of storage node -  dbSource -> physicalDbMachine assignment
   * Performs a join and produces a map physicalDbMachine,dbSource -> relay assignment
   *
   * @param dbName
   * @param relayReplicationFactor
   * @part relayNodes
   * @return
   */
  public IdealState calculateFinalIdealState(String dbName, List<String> relayNodes)
  throws Exception
  {
		  final List<String> storageNodes = _storageAdapter.getStorageNodes();

		  ZNRecord sevInit = _storageAdapter.getExternalView(false, dbName);
		  ZNRecord sev = computeFuncISandEVofStorageCluster(sevInit, dbName);

		  ZNRecordSerializer zs = new ZNRecordSerializer();
		  if (LOG.isDebugEnabled()) {
			  LOG.info("The external view for storage node is :");
			  LOG.info(new String(zs.serialize(sev)));
		  }

		  Map<String, Map<String, String>> cis = StorageAdapter.getCalculatedIdealState(relayNodes, storageNodes, _replicationFactor);
		  if (LOG.isDebugEnabled()) {
			  LOG.info("The calculated ideal state is ");
			  LOG.info(cis);
		  }

		  IdealState is = performJoin(cis, sev);
		  LOG.info("Join of calculated ideal state and external view from storage node ");
		  LOG.info(new String(zs.serialize(is.getRecord())));


		  return is;
  }

  /**
   *
   * @param relayClusterName
   * @param zr
   * @param client
   * @throws Exception
   */
  public void writeIdealStateToClusterManager(String dbName, IdealState is)
  throws Exception {

	  _relaySpectator.writeIdealStateToClusterManager(dbName, is);
	  LOG.info("Write storage configs to Relay cluster");
	  writeStorageConfigsToRelayCluster(dbName);

	  return;
  }

  protected static String[] parsePhyPartition(String phyPartition)
  {
	  String[] newStr = new String[2];
	  int lastIndex = StringUtils.lastIndexOf(phyPartition, '_');
	  newStr[0] = phyPartition.substring(0, lastIndex);
	  newStr[1] = phyPartition.substring(lastIndex+1, phyPartition.length());
	  return newStr;
  }

  /**
   * Used for joining external view and initial ideal state
   * @param ris
   * @param sev
   * @return
   */
  private IdealState performJoin(Map<String, Map<String, String>> risMapFields, ZNRecord sev)
  {
	  Map<String, Map<String, String>> relayIdealStateMV = new HashMap<String, Map<String,String>>();

	  LOG.info("Join Initial Relay ideal state with Storage external view");

	  Map<String, Map<String, String>> sevMapFields = sev.getMapFields();
	  for (Iterator<String> it = sevMapFields.keySet().iterator(); it.hasNext();)
	  {
		  String phyPartition = it.next();
		  Map<String, String> storageNodeInfoMap = sevMapFields.get(phyPartition);

		  LOG.info("Processing key: " + phyPartition);

		  /// Process key
		  String[] parts = phyPartition.split("_");
		  if (parts.length < 2){
			  LOG.error("Invalid key should resolve to atleast two subsbtrings, instead has " + parts.length + " offending key " + phyPartition);
			  continue;
		  }
		  String[] res  = parsePhyPartition(phyPartition);
		  String dbName = res[0];
		  int pNum = Integer.parseInt(res[1]);

		  String lp = "p" + pNum + "_1";


          /// Iterate through storage nodes for this partition (key here)
		  for (Iterator<String> it2 = storageNodeInfoMap.keySet().iterator(); it2.hasNext();)
		  {
			  String ps = it2.next();
			  String snState = storageNodeInfoMap.get(ps);

			  /// Construct ResourceKey
			  String resourceKey = ps + "," + dbName + "," + lp + "," + snState;
			  LOG.info("Processing resource = " + resourceKey);

			  Map<String, String> relaySet = risMapFields.get(ps);

			  Map<String, String> relaysWithStateForGivenResourceKeyMapView = new HashMap<String, String>();
			  List<String> relaysForGivenResourceKeyListView = new ArrayList<String>();
			  for (Iterator<String> j = relaySet.keySet().iterator(); j.hasNext();)
			  {
				  String relayName   = j.next();
				  String relayState = relaySet.get(relayName);
				  relaysForGivenResourceKeyListView.add(relayName);
				  relaysWithStateForGivenResourceKeyMapView.put(relayName, relayState);
			  }

			  relayIdealStateMV.put(resourceKey, relaysWithStateForGivenResourceKeyMapView);

		  }
	  }


	  IdealState finalRelayIdealState = new IdealState(sev.getId());
	  finalRelayIdealState.getRecord().getSimpleFields().putAll(sev.getSimpleFields());
	  finalRelayIdealState.getRecord().setMapFields(relayIdealStateMV);
	  finalRelayIdealState.setStateModelDefRef("OnlineOffline");
	  finalRelayIdealState.setIdealStateMode(IdealStateModeProperty.CUSTOMIZED.toString());
	  finalRelayIdealState.setNumPartitions(relayIdealStateMV.size());
	  finalRelayIdealState.setReplicas(Integer.toString(_replicationFactor));
	  return finalRelayIdealState;
  }

  /**
   * This is typically used to read mysql configurations from storage cluster
   * and write them to relay cluster for use by REPL_DBUS
   *
   * @throws Exception
   */
  protected void writeStorageConfigsToRelayCluster(String dbName)
  throws Exception
  {
	  // Read mysql configs from storage cluster
	  Map<String, String> storageConfigs = _storageAdapter.readStorageConfigs();
	  _relaySpectator.writeStorageNodeConfigs(storageConfigs);
	  
	  // Read number of partitions on storage cluster
	  int numPartitions = _storageAdapter.getNumPartitions(dbName);
	  String key = dbName + Constants.NUM_PARTITIONS_SUFFIX;
	  _relaySpectator.writeNumPartitions(key, numPartitions);
  }

  @Override
  public void handleDatabaseAdditionRemoval(Set<String> dbNames)
  throws Exception
  {
	  Set<String> addedDatabases = new HashSet<String>(dbNames);
	  addedDatabases.removeAll(_dbNames);
	  for (String dbName: addedDatabases )
	  {
		  assert(_dbNames.contains(dbName) == false);
		  LOG.info("Add dbName " + dbName + " to current list of dbNames " + _dbNames);
		  _dbNames.add(dbName);
	  }
	  
	  Set<String> deletedDatabases = new HashSet<String>(_dbNames);
	  deletedDatabases.removeAll(dbNames);
	  for (String dbName: deletedDatabases )
	  {
		  assert(_dbNames.contains(dbName) == true);
		  LOG.info("Remove dbName " + dbName + " from current list of dbNames " + _dbNames);
		  _relaySpectator.removeResourceGroup(dbName);
		  _rm.dropDatabase(dbName);
		  _dbNames.remove(dbName);
	  }
	  
	  return;
  }
  
  /**
   * Retrigger ideal state computation when a storage node external view changes
   */
  @Override
  public void onExternalViewChange(String dbName, ExternalView externalView)
  throws Exception
  {
	 LOG.info("Starting an update of ideal state because of a storage node external view change ");
	 updateIdealState(dbName, externalView);

	 return;
  }
  
  /**
   *
   * @return
   */
  protected ZNRecord computeFuncISandEVofStorageCluster(ZNRecord sev, String dbName)
  throws NotAReplicatingResourceException
  {
	  ZNRecord sis = null;
	  if (null != _storageAdapter)
	  {
		  sis = _storageAdapter.getIdealState(dbName);
	  }

	  if ( sis == null)
	  {
		  LOG.error("IdealState of storage cluster is empty, which is an error case");
		  return new ZNRecord(dbName);
	  }

	  Map<String, Map<String, String>> funcEvIs = new HashMap<String, Map<String,String>>();

	  /**
	   * Iterate through the entries in IdealState on a per partition basis.
	   * (1) If the entry exists in external view, use it
	   * (2) If it does not, use it from IdealState, but set the state as OFFLINE
	   */
	  for (Map.Entry<String, Map<String, String>> it : sis.getMapFields().entrySet())
	  {
		  String partitionKey = it.getKey();

		  Map<String, String> mfsev = null;
		  if ( null != sev )
		  {
			  mfsev = sev.getMapField(partitionKey);
		  }

		  assert( null != sis);
		  Set<String> partitionInstanceSet ;
		  String isMode = sis.getSimpleField(IdealState.IdealStateProperty.IDEAL_STATE_MODE.toString());
		  if(null != isMode && isMode.equals(IdealState.IdealStateModeProperty.CUSTOMIZED.toString())){
			  partitionInstanceSet= sis.getMapField(partitionKey).keySet();
		  }else{
			   partitionInstanceSet = new HashSet<String>(sis.getListField(partitionKey));
		  }
		  Map<String, String> funcEvIsMf = new HashMap<String, String>();
		  for (String sn: partitionInstanceSet)
		  {
			  String state = "OFFLINE";
			  if (null != mfsev && mfsev.containsKey(sn))
			  {
				  state = mfsev.get(sn);
			  }
			  funcEvIsMf.put(sn, state);
		  }
		  funcEvIs.put(partitionKey, funcEvIsMf);
	  }

	  ZNRecord funcEvIsZr = new ZNRecord(sis.getId());
	  funcEvIsZr.setMapFields(funcEvIs);
	  if (null != sis.getSimpleFields())
	      funcEvIsZr.getSimpleFields().putAll(sis.getSimpleFields());
	  ZNRecordSerializer zs = new ZNRecordSerializer();
	  LOG.info("SIS is " + new String(zs.serialize(sis)));
	  LOG.info("Computed function of IS and EV is " + new String(zs.serialize(funcEvIsZr)));
	  return funcEvIsZr;
  }

  protected Set<String> getDbNames()
  {
	  return _dbNames;
  }
}
