/**
 *
 */
package com.linkedin.databus3.espresso.cmclient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.cmclient.ResourceKey;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.relay.config.LogicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.SourceIdNameRegistry;
import com.linkedin.databus3.espresso.schema.EspressoBackedSchemaRegistryService;
import com.linkedin.databus3.espresso.schema.EspressoSchemaName;

/**
 * @author pganti
 *
 */
public abstract class BasePhysicalSourceConfigBuilder {


	/**
	 * Registry that provides mapping between logical source and a numerical id
	 */
	protected volatile SourceIdNameRegistry _sourceIdNameRegistry = null;
	protected EspressoBackedSchemaRegistryService _schemaRegistryService = null;
	protected final String MODULE = BasePhysicalSourceConfigBuilder.class.getName();
	protected final Logger LOG = Logger.getLogger(MODULE);
	private final ConcurrentHashMap<String, List<String>> _dbToTablesList;
	protected ConcurrentHashMap<String, Integer> _cacheHitCount;

	BasePhysicalSourceConfigBuilder()
	{
		_dbToTablesList = new ConcurrentHashMap<String, List<String>>();
		_cacheHitCount = new ConcurrentHashMap<String, Integer>();
	}

	// create one config from the key
	protected PhysicalSourceConfig buildOnePSSCFromResourceKey(ResourceKey rk) throws InvalidConfigException {
	  final String ps = rk.getPhysicalSource();
	  final String pp = rk.getPhysicalPartition();
	  final int lpNum = rk.getLogicalPartitionNumber();
	  //final boolean isMaster = rk.isMaster();

	  // True for Espresso case only
	  final String dbName = pp;

	  /**
	   * Parameters to construct PhysicalSourceConfig object
	   * Hash based on pp_lpNum (e.g., COMM_1, as that corresponds to one MySQL database and needs a clock )
	   */

	  final String uri = ps;

	  final int partitionId = lpNum;

	  PhysicalSourceConfig psc = new PhysicalSourceConfig(pp, uri, partitionId);
	  //psc.setRole(isMaster? PhysicalSource.PHYSICAL_SOURCE_MASTER : PhysicalSource.PHYSICAL_SOURCE_SLAVE);
	  psc.setRole(rk.getRoleString());
	  addLogicalSource(psc, dbName, ps, pp, lpNum);

	  return psc;
	}

	// used for unit testing only
	protected List<PhysicalSourceStaticConfig> buildPSSCFromResourceKeys(Vector<ResourceKey> rks)
	throws InvalidConfigException
	{
		// create a list fo new partitions configs
		Map<String, PhysicalSourceConfig> nameToPhySourceConfig = new HashMap<String, PhysicalSourceConfig>();
		for(int i = 0; i < rks.size(); ++i) {
		  PhysicalSourceConfig psc = buildOnePSSCFromResourceKey(rks.elementAt(i));
		  //String sourceName = psc.getName();
		  String sourceName = psc.getName() + "_" + Integer.toString(psc.getId());

			if (nameToPhySourceConfig.containsKey(sourceName)) {
				LOG.info("Duplicate occurrence physicalPartition_partitionNumber. Ignoring sourceName = " + sourceName);
			} else {
				LOG.info("Creating a physical source config element for sourceName = " + sourceName);
				nameToPhySourceConfig.put(sourceName, psc);
			}
		}
		LOG.info("ResourceKey size = " + rks.size() + " Num of phySourceConfigs created = " + nameToPhySourceConfig.size());

		List<PhysicalSourceStaticConfig> psscList = new ArrayList<PhysicalSourceStaticConfig>(nameToPhySourceConfig.size());
		for (Map.Entry<String, PhysicalSourceConfig> entry: nameToPhySourceConfig.entrySet()) {
			psscList.add(entry.getValue().build());
		}
		return psscList;
	}

	private void addLogicalSource(PhysicalSourceConfig psc, String dbName, String ps, String pp, int lpNum)
	throws InvalidConfigException {

		/**
		 * Logical source parameters
		 */
		final int    l_partitionId = lpNum;
		final String l_uri = ps;

		/**
		 *  Get all logical sources from Espresso Schema Registry
		 */
		final List<String> logicalSourceNames = getLogicalSourcesFromRegistry(dbName);

		for (String ls: logicalSourceNames) {
			LOG.info("Adding logical source" + ls);
			long id = getLogicalSourceId(ls);
			String l_name = getLogicalSourceName(ls, lpNum);
			/**
			 * Add all the logical sources
			 */
			LogicalSourceConfig lsc = new LogicalSourceConfig();
			lsc.setId((short)id);
			lsc.setName(l_name);
			lsc.setPartition((short)l_partitionId);
			lsc.setPartitionFunction("constant:1");
			lsc.setUri(l_uri);
			// Not keeping Uri same as l_name like in V2, because we know the exact physical address now
			//lsc.setUri(l_name);
			psc.addSource(lsc);
		}

	}

	/**
	 * Obtain a list of tables for every database only once. Cache them subsequently, to prevent multiple calls to
	 * schema registry service
	 * When there is a new event for a table, as part of event injection into relay buffer, we perform a blocking
	 * schema refresh update
	 *
	 * @param dbName
	 * @return
	 * @throws InvalidConfigException
	 */
	protected List<String> getLogicalSourcesFromRegistry(String dbName)
	throws InvalidConfigException
	{
		try {
			List<String> lss = new ArrayList<String>();
			if (_dbToTablesList.containsKey(dbName))
			{
				_cacheHitCount.put(dbName, _cacheHitCount.get(dbName)+1);
				lss = _dbToTablesList.get(dbName);
			}
			else
			{
				// Generate sourceIds and Schemas for the database
				_schemaRegistryService.loadAllSourcesOnlyIfNewEspressoDB(dbName);
			    lss = _schemaRegistryService.getAllLogicalSourcesInDatabase(dbName);

				_cacheHitCount.put(dbName, 0);
			    _dbToTablesList.put(dbName, lss);
			}
			return lss;
		} catch (DatabusException e)
		{
			throw new InvalidConfigException(e);
		}
	}

	protected long getLogicalSourceId(String name)
	throws InvalidConfigException {
		/**
		 * Normal workflow
		 */
		try {
			if (null != _sourceIdNameRegistry) {
				EspressoSchemaName es = EspressoSchemaName.create(name);
				LOG.info("getLogicalSourceId for name = " + name + " modName = " + es.getDatabusSourceName());
				Integer id =  _sourceIdNameRegistry.getSourceId(es.getDatabusSourceName());
				if (null != id) {
					return id.longValue();
				}
				else {
					// update the sourceId mapping from db to table, and retry
					_schemaRegistryService.loadAllSourcesOnlyIfNewEspressoDB(es.getDbName(), false);
					id =  _sourceIdNameRegistry.getSourceId(es.getDatabusSourceName());
					if (null != id)
					{
						LOG.info("Obtained id for " + es.getDatabusSourceName() + " after a sourceId refresh");
						return id.longValue();
					}
					throw new InvalidConfigException("Could not get an ID from sourceId registry for logical source " + name);
				}
			}
			else {
				throw new InvalidConfigException("SourceIdRegistry not initialized");
			}
		} catch (DatabusException e)
		{
			throw new InvalidConfigException(e);
		}
	}

	private String getLogicalSourceName(String databusSourceName, int lpNum)
	throws InvalidConfigException
	{
		String l_name = databusSourceName;
		try {
			EspressoSchemaName es = EspressoSchemaName.create(databusSourceName);
			l_name = es.getDbName() + "." + es.getTableName();
		} catch (DatabusException e)
		{
			throw new InvalidConfigException(e);
		}
		return l_name;
	}
}
