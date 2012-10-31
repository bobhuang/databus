package com.linkedin.databus3.espresso.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;

import com.linkedin.databus.core.DbusConstants;
import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.schemas.NoSuchSchemaException;
import com.linkedin.databus2.schemas.SchemaId;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.SourceIdNameRegistry;
import com.linkedin.databus2.schemas.VersionedSchema;
import com.linkedin.databus2.schemas.VersionedTableSchemaLookup;
import com.linkedin.espresso.common.api.EspressoException;
import com.linkedin.espresso.schema.EspressoSchemaRegistry;
import com.linkedin.espresso.schema.SchemaManagerFactory;
import com.linkedin.espresso.schema.SchemaRegistry;
import com.linkedin.espresso.schema.api.DocumentSchema;
import com.linkedin.espresso.schema.api.SchemaNotFoundException;
import com.linkedin.espresso.schema.api.TableSchema;
import com.linkedin.espresso.schema.impl.SchemaConstants;

public class EspressoBackedSchemaRegistryService implements SchemaRegistryService
{
  public static final String MODULE = EspressoBackedSchemaRegistryService.class.getName();
  public static final String PERF_MODULE = MODULE + "Perf";
  public static final Logger LOG = Logger.getLogger(MODULE);
  public static final Logger PERF_LOG = Logger.getLogger(PERF_MODULE);

  private final SchemaRegistry.StaticConfig _config;
  private final EspressoSchemaRegistry _schemaRegistry;
  private final SourceIdNameRegistry _sourceIdRegistry;
  private final Set<String> _knownDatabases = Collections.synchronizedSet(new HashSet<String>());
  private final Map<VersionedTableSchemaLookup, SchemaId> _logicalSrcName2SchemaId =
		  new ConcurrentHashMap<VersionedTableSchemaLookup, SchemaId>();

  private Thread _schemaSetRefreshThread;
  private final AtomicBoolean _stopRefreshThread = new AtomicBoolean();
  private long _refreshPeriodInMs = 0;

  public EspressoBackedSchemaRegistryService(SchemaRegistry.StaticConfig config,
                                             SourceIdNameRegistry sourceIdRegistry)
         throws InvalidConfigException, DatabusException
  {
    _config = config;
    _sourceIdRegistry = sourceIdRegistry;
    try
    {
      _schemaRegistry = SchemaManagerFactory.getSchemaManager(_config);
    }
    catch (EspressoException e1)
    {
      throw new DatabusException("Unable to create espresso schema registry: " + e1.getMessage(), e1);
    }
    catch (com.linkedin.espresso.common.config.InvalidConfigException e1)
    {
      throw new InvalidConfigException("invalid espresso schema registry configuration: " + e1.getMessage(), e1);
    }
    _refreshPeriodInMs = config.getRefreshPeriodMs();
    if (null == _schemaRegistry)
    {
      throw new InvalidConfigException("unknown schema registry mode: " + _config.getMode());
    }

    try
    {
      _schemaRegistry.start();
      if (_refreshPeriodInMs > 0)
      {
         LOG.info("SchemaRegistry refresh period in ms is " + _refreshPeriodInMs);
         startSchemasRefreshThread();
      }
    }
    catch (Exception e)
    {
      LOG.error("unable to start espresso schema registry: " + e.getMessage());
      throw new DatabusException("unable to start espresso schema registry", e);
    }
  }

  /**
   * Shutdown the service
   * Currently only involves shutting down the update thread
   */
  public void shutdown()
  {
	  stopSchemasRefreshThread();
  }

  @Override
  public void registerSchema(VersionedSchema schema)
         throws DatabusException
  {
    throw new DatabusException("not implemented");
  }

  @Override
  public String fetchSchema(String schemaId) throws NoSuchSchemaException,
      DatabusException
  {
    throw new DatabusException("not implemented");
  }

  /**
   * A databusSourceName is of the form EspressoDB.TableName
   * E.g., EspressoDB.Email
   *
   * If there is no such databusSourceNamem throws a NoSuchSchemaException
   * Guaranteed to not return null
   */
  @Override
  public String fetchLatestSchemaByType(String databusSourceName)
  throws NoSuchSchemaException, DatabusException
  {
    Schema s = fetchLatestSchemaObjByType(databusSourceName);
    assert( s != null);
    return s.toString();
  }

  @Override
  public Map<Short, String> fetchAllSchemaVersionsByType(String databusSourceName)
  throws NoSuchSchemaException, DatabusException
  {
      EspressoSchemaName schemaName = EspressoSchemaName.create(databusSourceName);
      loadAllSourcesOnlyIfNewEspressoDB(schemaName.getDbName());
	  return obtainAllSchemasForDatabusSourceName(databusSourceName);
  }

  /**
   * Accessor for the logicalSourceName=>schemaId mapping
   * Usage : databusSourceName -> EspressoDB.TableName
   * Throws NoSuchSchemaException if there is no such schema, else returns the schemaId
   * Please note that if the dbName and table exist, but not the version number, then it throws an exception
   */
  @Override
  public SchemaId fetchSchemaIdForSourceNameAndVersion(String databusSourceName, int version)
  throws DatabusException {
	VersionedTableSchemaLookup vts = new VersionedTableSchemaLookup(databusSourceName, version);
	SchemaId sid = _logicalSrcName2SchemaId.get(vts);
    if (null == sid )
    {
        loadSchemaId(databusSourceName, version);
    	sid = _logicalSrcName2SchemaId.get(vts);
        if (null == sid)
        {
        	String errMsg = "Do not have a schema for VersionedTableSchema " + databusSourceName + " version " + version;
        	LOG.error(errMsg + vts);
        	throw new DatabusException(errMsg);
        }
    }
    return sid;
  }

  @Override
  public VersionedSchema fetchLatestVersionedSchemaByType(String eventType)
  throws NoSuchSchemaException, DatabusException {
	  throw new DatabusException("not implemented");
  }

  /**
   * The semantics of this function are :
   * 1. Obtain the latest schema of a given (EspressoDB,TableName)
   * 2. Load the sourceIds for all the tables of (EspressoDB) into SourceIdRegistry
   *
   * @param databusSourceName - Of the form EspressoDB.TableName
   * @return
   * @throws NoSuchSchemaException
   * @throws DatabusException
   */
  public Schema fetchLatestSchemaObjByType(String databusSourceName)
  throws DatabusException
  {
    long startTs = System.nanoTime();
    EspressoSchemaName schemaName = EspressoSchemaName.create(databusSourceName);
    loadAllSourcesOnlyIfNewEspressoDB(schemaName.getDbName());

    DocumentSchema docSchema = null;
    try
    {
      TableSchema tabSchema = _schemaRegistry.getTableSchema(schemaName.getDbName(),
                                                             schemaName.getTableName());
      String docSchemaName = tabSchema.getDocumentSchemaName();
      docSchema = _schemaRegistry.getLatestDocumentSchema(schemaName.getDbName(),
                                                          docSchemaName);

    }
    catch (SchemaNotFoundException se)
    {
      LOG.error("fetchLatestSchemaObjByType has error for databusSourceName = "  + databusSourceName, se);
      throw new NoSuchSchemaException(se.getMessage());
    }
    catch (EspressoException ee)
    {
      throw new DatabusException("Error getting espresso schema for " + databusSourceName + ": " +
                                 ee.getMessage(), ee);
    }

    if (PERF_LOG.isDebugEnabled())
        PERF_LOG.debug("fetchLatestSchemaObjByType took : " + ((System.nanoTime()-startTs)*1.0) / DbusConstants.NUM_NSECS_IN_MSEC + "ms");
    return docSchema.getAvroSchema();
  }

  /**
   * For a given database
   * 1. Generates source Ids for all its tables
   * 2. For each table, extracts all of its document schemas and computes schemaId, if it was not done so before
   * 3. Caches the schemaIds for faster lookup in future
   *
   *  The above operations are NOT done even if the database is known before. Therefore this call does NOT ensure that
   *  the schemas / sourceIds are updated. In fact, if new tables are added, a call to this method will definitely not
   *  reflect it
   *
   *  This method is supposed to be invoked during
   *  1. Initialization of Relay
   *  2. Helix State transitions when the first call updates sourceIds / schemaId cache, but subsequent calls are no-ops
   *     and incur no synchronization overhead.
   *
   * @param dbName
   * @throws DatabusException
   */
  public void loadAllSourcesOnlyIfNewEspressoDB(String dbName)
  throws DatabusException
  {
    loadAllSourcesOnlyIfNewEspressoDB(dbName, true);
  }

  /**
   * This method is a variant, where source Ids are generated for all Espresso databases
   * Used, in cases, where new tables may have been added after the initial sources been created
   *
   */
  public void loadAllSourcesOnlyIfNewEspressoDB(String dbName, boolean cached)
  throws DatabusException
  {
      LOG.debug("Invoking loadAllSourcesOnlyIfNewEspressoDB for dbName" + dbName);
      if (_knownDatabases.contains(dbName) && cached)
      {
          LOG.debug("The database " + dbName + " is in the known list of databases. Skipping");
          return;
      }
      generateAllSourceIdsAndAllSchemaIdsForEspressoDB(dbName);

      _knownDatabases.add(dbName);
      LOG.debug("Added dbName for knownDatabases list" + dbName);
  }

  /**
   * For a given EspressoDB, returns all the tables present in it
   *
   * @param dbName
   * @return
   * @throws DatabusException
   */
  public List<String> getAllLogicalSourcesInDatabase(String dbName)
  throws DatabusException
  {
	  long startTs = System.nanoTime();
	  List<String> lss = new ArrayList<String>();

	  try {
		  List<TableSchema> tableSchemas = new ArrayList<TableSchema>(_schemaRegistry.getAllTableSchemas(dbName));

		  for (TableSchema ts: tableSchemas)
		  {
			  EspressoSchemaName schemaName = EspressoSchemaName.create( dbName, ts.getName());
			  lss.add(schemaName.getDatabusSourceName());
			  if (LOG.isDebugEnabled())
			  {
				  LOG.debug("Adding schema " + schemaName.getDatabusSourceName());
			  }
		  }
	  }
	  catch (SchemaNotFoundException se)
	  {
	    LOG.error("Error getting all logical sources in database "  + dbName, se);
	    return lss;
	  }
	  catch (EspressoException e)
	  {
		  throw new DatabusException("Error generating espresso logical sources per dbName" + e.getMessage(), e);
	  }
	  if (PERF_LOG.isDebugEnabled())
	  {
          PERF_LOG.debug("getAllLogicalSourcesInDatabase took : " + ((System.nanoTime()-startTs)*1.0) / DbusConstants.NUM_NSECS_IN_MSEC + "ms");
	  }
	  return lss;
  }

  @Override
  public void dropDatabase(String dbName) throws DatabusException 
  {
	  LOG.info("Dropping cached schemas for dbName" + dbName);
	  _knownDatabases.remove(dbName);
	  
	  Set<Map.Entry<VersionedTableSchemaLookup, SchemaId>> entrySet = _logicalSrcName2SchemaId.entrySet();
	  for (Map.Entry<VersionedTableSchemaLookup, SchemaId> vt: entrySet)
	  {
		  String databusSourceName = vt.getKey().getDatabusSourceName();
          EspressoSchemaName schemaName = EspressoSchemaName.create(databusSourceName);
		  if (schemaName.getDbName().equals(dbName))
		  {
			  entrySet.remove(vt);
		  }
	  }
	  return;
  }
  
  /**
   * Forces generating sourceIds and schemaIds
   * @param dbName
   * @throws DatabusException
   */
  private void generateAllSourceIdsAndAllSchemaIdsForEspressoDB(String dbName)
  throws DatabusException
  {
	  generateAllSourceIdsForEspressoDB(dbName);
	  generateAllSchemaIdsForEspressoDB(dbName);
  }

  /**
   * For a given EspressoDB,table,version do
   * 1. Load all sourceIds for EspressoDB onto sourceIdRegistry
   * 2. Load the schema for triad (EspressoDB,table,version) schemaId onto cache
   *
   * @param lSourceName : Of the form EspressoDB.TableName
   * @param version
   * @throws DatabusException
   */
  private void loadSchemaId(String databusSourceName, int version)
  throws DatabusException
  {
      EspressoSchemaName schemaName = EspressoSchemaName.create(databusSourceName);
      String dbName = schemaName.getDbName();
      // Generating and updating sourceIds occurs at a DB level granularity for efficiency
      // While we just needed to loadSourceId for a given table version, we therefore update for whole DB
	  generateAllSourceIdsForEspressoDB(dbName);

	  // Updating schemaId occurs at a given table /version level as it computes md5 hash of schema and is
	  // therefore expensive
	  generateSchemaIdForDatabusSourceNameAndVersion(databusSourceName, version);
  }

  /**
   *
   * For a given Espresso database :
   * 1. Generate a schemaId for all of its tables
   * 2. Cache the logicalSourceName to Schema id mapping
   *
   * @param dbName
   * @throws DatabusException
   */
  private void generateAllSchemaIdsForEspressoDB(String dbName)
  throws DatabusException
  {
	  long startTs = System.nanoTime();
	  if (LOG.isDebugEnabled())
	  {
		  LOG.debug("generating logical source ids for Espresso database: " + dbName);
	  }
	  try
	  {
		  List<TableSchema> tableSchemas = new ArrayList<TableSchema>(_schemaRegistry.getAllTableSchemas(dbName));

		  for (TableSchema tableSchema: tableSchemas)
		  {
		    String docSchemaName = null;
		    try
		    {
			  docSchemaName = tableSchema.getDocumentSchemaName();
			  DocumentSchema latestDocSchema = _schemaRegistry.getLatestDocumentSchema(dbName, docSchemaName);
			  int maxVersion = latestDocSchema.getVersion();

			  for (int version = 1; version < maxVersion; version++)
			  {
			    String dbSourceName = null;
			    try
			    {
			      EspressoSchemaName schemaName = EspressoSchemaName.create(dbName, tableSchema.getName());
			      dbSourceName = schemaName.getDatabusSourceName();
			      generateSchemaIdForDatabusSourceNameAndVersion(dbSourceName, version);
			    } catch (NoSuchSchemaException innerSe)
			    {
			      // Log error, but continue with generating schema ids for other versions if possible
			      LOG.error("Error generating SchemaId for databusSourceName " + dbSourceName + " version " + version);
			    }
			  }
		    } catch (SchemaNotFoundException se)
		    {
		      LOG.error("Document Schema not found for " + dbName + " docSchemaName " + docSchemaName, se);
		    }
		  }
	  }  catch (EspressoException e) {
		  String errMsg = "Error generating espresso schemaId for db:" + dbName;
		  LOG.error(errMsg, e);
		  throw new DatabusException(errMsg);
	  }
	  if (PERF_LOG.isDebugEnabled())
	  {
		  PERF_LOG.debug("generateAllSchemaIdsForEspressoDB took : " + ((System.nanoTime()-startTs)*1.0) / DbusConstants.NUM_NSECS_IN_MSEC + "ms");
	  }
  }

  /**
   *
   * For a given Espresso database, generate a schemaId for all of its tables
   * logicalSourceName to Schema id mapping
   * @param databusSourceName
   * @param version
   * @throws DatabusException
   */
  private void generateSchemaIdForDatabusSourceNameAndVersion(String databusSourceName, int version)
  throws DatabusException
  {
	  long startTs = System.nanoTime();
	  EspressoSchemaName schemaName = EspressoSchemaName.create(databusSourceName);
	  String dbName = schemaName.getDbName();
	  String tableName = schemaName.getTableName();

	  if (LOG.isDebugEnabled())
	  {
		  LOG.debug("generateSchemaIdForDatabusSourceNameAndVersion: databusSourceName = " + dbName + " version = " + version);
	  }
	  try
	  {
		  VersionedTableSchemaLookup vts = new VersionedTableSchemaLookup(databusSourceName, version);
		  if (! _logicalSrcName2SchemaId.containsKey(vts))
		  {
		    try
		    {
			  TableSchema tableSchema = _schemaRegistry.getTableSchema(dbName, tableName);
			  String docSchemaName = tableSchema.getDocumentSchemaName();
			  DocumentSchema docSchema = _schemaRegistry.getDocumentSchemaByVersion(dbName, docSchemaName, version);
			  if (null == docSchema || null == docSchema.getAvroSchema())
			  {
			      String errMsg = "Got null docSchema for dbName=" + dbName + " docSchemaName=" + docSchemaName +  " version = " + version;
				  LOG.error(errMsg);
				  throw new SchemaNotFoundException(errMsg);
			  }

			  SchemaId schemaId = SchemaId.forSchema(docSchema.getAvroSchema());
			  _logicalSrcName2SchemaId.put(vts, schemaId);
			  if (LOG.isDebugEnabled())
			  {
				  LOG.debug("logical src name=" + schemaName.getDatabusSourceName() + " maps to " + schemaId.toString());
			  }
		    } catch (SchemaNotFoundException se)
		    {
		      LOG.error("generateSchemaIdForDatabusSourceNameAndVersion got schemaNotFoundException for dbName " + dbName + " tableName " + tableName, se);
		      throw new NoSuchSchemaException(se.getMessage());
		    }
		  }

	  } catch (EspressoException e) {
		  String errMsg = "Could not generate espresso schemaId for databusSourceName:" + databusSourceName + " version: " + version;
		  LOG.error(errMsg, e);
		  throw new DatabusException(errMsg);
	  }
	  if (PERF_LOG.isDebugEnabled())
	  {
		  PERF_LOG.debug("generateSchemaIdForDatabusSourceNameAndVersion took : " + ((System.nanoTime()-startTs)*1.0) / DbusConstants.NUM_NSECS_IN_MSEC + "ms");
	  }
  }

  /**
   * Given an EspressoDB, generate and load sourceIds for all of its tables to SourceIdRegistry
   *
   * @param dbName
   * @throws DatabusException
   */
  private void generateAllSourceIdsForEspressoDB(String dbName)
  throws DatabusException
  {
    long startTs = System.nanoTime();
    LOG.debug("generating logical source ids for Espresso database: " + dbName);
    try
    {
      ArrayList<LogicalSource> newSources = new ArrayList<LogicalSource>(_sourceIdRegistry.getAllSources());

      List<TableSchema> tableSchemas = new ArrayList<TableSchema>(_schemaRegistry.getAllTableSchemas(dbName));

      for (TableSchema tableSchema: tableSchemas)
      {
        try
        {
          EspressoSchemaName schemaName = EspressoSchemaName.create(dbName, tableSchema.getName());

          int sourceId;

          // get logical source id (table schema) by db/table/version
          sourceId = _schemaRegistry.getIdForTableSchema(dbName, tableSchema.getName(), SchemaConstants.MIN_VERSION);
          if (LOG.isDebugEnabled())
          {
            LOG.debug("Getting source id for sourceName=" + schemaName.getDatabusSourceName() + ";srcId=" + sourceId);
          }

          LogicalSource ls = new LogicalSource(Integer.valueOf(sourceId),
                                               schemaName.getDatabusSourceName());
          newSources.add(ls);
        } catch (EspressoException ee)
        {
          LOG.error("Error getting id for table schema" + tableSchema.getName());
        }
      }

      _sourceIdRegistry.update(newSources);
    }
    catch (SchemaNotFoundException se)
    {
      LOG.error("Cannot find schema for dbName "+ dbName);
    }
    catch (EspressoException e)
    {
      throw new DatabusException("error generating espresso logical source ids: ",e);
    }
    if (PERF_LOG.isDebugEnabled())
    {
        PERF_LOG.debug("generateSourceIdsForEspressoSchemas took : " + ((System.nanoTime()-startTs)*1.0) / DbusConstants.NUM_NSECS_IN_MSEC + "ms");
    }
  }

  /**
   * Starts the thread that periodically ({@link Config#getRefreshPeriodMs()} refreshes the schema set.
   * @return true if started
   */
  private boolean startSchemasRefreshThread()
  {
    if (_refreshPeriodInMs <= 0 || null != _schemaSetRefreshThread)
    {
      return false;
    }
    LOG.info("Starting Espresso backed schema refresh thread");

    _stopRefreshThread.set(false);
    _schemaSetRefreshThread = new Thread(new SchemaSetRefreshThread(), "SchemaRefreshThread");
    _schemaSetRefreshThread.setDaemon(true);
    _schemaSetRefreshThread.start();

    return true;
  }

  /**
   * To be invoked at the time of shutting down the service
   */
  private void stopSchemasRefreshThread()
  {
    if (null == _schemaSetRefreshThread) return;

    LOG.info("Stopping schema refresh thread");
    _stopRefreshThread.set(true);
    _schemaSetRefreshThread.interrupt();
    while (_schemaSetRefreshThread.isAlive())
    {
      try
      {
        _schemaSetRefreshThread.join();
      }
      catch (InterruptedException ie) {}
    }
  }

  /**
   * Obtains all schemas for a particular logical source
   * @param databusSourceName Represented as EspressoDB.TableName
   * @return map of version number to id for all document schemas for the table
   */
  private HashMap<Short, String> obtainAllSchemasForDatabusSourceName(String databusSourceName)
  {
	  HashMap<Short, String> result = new HashMap<Short, String>();

	  try
	  {
		  if (LOG.isDebugEnabled())
		  {
			  LOG.debug("Updating schema versions for eventType " + databusSourceName);
		  }

		  EspressoSchemaName schemaName = EspressoSchemaName.create(databusSourceName);

		  DocumentSchema latestSchema = null;
		  TableSchema tabSchema = _schemaRegistry.getTableSchema(schemaName.getDbName(),
				  schemaName.getTableName());
		  String docSchemaName = tabSchema.getDocumentSchemaName();
		  latestSchema = _schemaRegistry.getLatestDocumentSchema(schemaName.getDbName(),
				  docSchemaName);

		  for (int i = 1; i <= latestSchema.getVersion(); ++i)
		  {
			  DocumentSchema schema = _schemaRegistry.getDocumentSchemaByVersion(schemaName.getDbName(), docSchemaName, i);
			  if (null != schema) result.put((short)i, schema.getAvroSchema().toString());
		  }

		  //
		  if (LOG.isDebugEnabled())
		  {
			  LOG.debug("Calling obtainAllSchemasForDatabusSourceName");
		  }
	  }
	  catch (SchemaNotFoundException se)
	  {
	    LOG.error("obtainAllSchemasForDatabusSourceName has error obtaining schema for " + databusSourceName,  se);
	  }
	  catch (EspressoException ee)
	  {
		  LOG.error("Error getting espresso schema for " + databusSourceName + ": " + ee.getMessage(), ee);
	  }
	  catch (DatabusException de)
	  {
	    LOG.error("Got DatabusException while getting all schemas for a databus sourcename" + de.getMessage(), de);
	  }
	  return result;
  }

  /**
   * Updates schemas for all the logical sources in the source id registry
   */
  private void refreshSchemaSet()
  {
	if (LOG.isDebugEnabled())
	{
		LOG.debug("Refreshing schema registry");
	}

	/**
	 * To be noted that initially, the list of sources will be empty
	 * as the relay can be started with no Databases
	 * There is NO API to query list of databases. So until we get either
	 * (1) The first helix notification regarding a dbName (or)
	 * (2) The first event for which we know the dbName
	 * refresh is a no-op.
	 *
	 */
	Set<String> knownDatabasesCopy = new HashSet<String>(_knownDatabases);
	for (String dbName : knownDatabasesCopy)
	{
		try
		{
			generateAllSourceIdsForEspressoDB(dbName);
			generateAllSchemaIdsForEspressoDB(dbName);
		} catch (DatabusException de)
		{
			LOG.error("Exception in schemas registry update", de);
		}
	}

    LOG.debug("schema registry refreshed");
  }


  private class SchemaSetRefreshThread implements Runnable
  {

    public SchemaSetRefreshThread()
    {
    }

    @Override
    public void run()
    {
      while (! _stopRefreshThread.get())
      {
        try
        {
          Thread.sleep(_refreshPeriodInMs);
        }
        catch (InterruptedException ie)
        {//do nothing
        }
        if (LOG.isDebugEnabled())
        {
            LOG.debug("Initiating another schema refresh");
        }
        refreshSchemaSet();
      }

      if (LOG.isDebugEnabled())
      {
          LOG.debug("Quitting schema refresh thread");
      }
    }

  }
}
