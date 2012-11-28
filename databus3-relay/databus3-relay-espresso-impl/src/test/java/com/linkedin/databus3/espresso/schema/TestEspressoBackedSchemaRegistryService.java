package com.linkedin.databus3.espresso.schema;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.schemas.NoSuchSchemaException;
import com.linkedin.databus2.schemas.SchemaId;
import com.linkedin.databus2.schemas.SourceIdNameRegistry;
import com.linkedin.databus2.schemas.VersionedTableSchemaLookup;
import com.linkedin.espresso.schema.SchemaRegistry;

public class TestEspressoBackedSchemaRegistryService
{
  public static final Logger LOG = Logger.getLogger(TestEspressoBackedSchemaRegistryService.class);

  private static final String SCHEMA_ROOTDIR_PROP_NAME = "espresso.schema.rootdir";
  private static final String DEFAULT_ESPRESSO_SCHEMA_ROOTDIR = "../databus3-relay-cmdline-pkg/espresso_schemas_registry";
  private final String SCHEMA_ROOTDIR;
  private final SchemaRegistry.Config _configBuilder;
  private final SchemaRegistry.StaticConfig _config;

  static
  {
    PatternLayout defaultLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c} %m%n");
    ConsoleAppender defaultAppender = new ConsoleAppender(defaultLayout);

    Logger.getRootLogger().removeAllAppenders();
    Logger.getRootLogger().addAppender(defaultAppender);

    Logger.getRootLogger().setLevel(Level.OFF);
    Logger.getRootLogger().setLevel(Level.ERROR);
    Logger.getRootLogger().setLevel(Level.INFO);
    //Logger.getRootLogger().setLevel(Level.DEBUG);
  }

  public TestEspressoBackedSchemaRegistryService() throws Exception
  {
    SCHEMA_ROOTDIR = System.getProperty(SCHEMA_ROOTDIR_PROP_NAME, DEFAULT_ESPRESSO_SCHEMA_ROOTDIR);
    LOG.info("Using espresso schema registry root: " + SCHEMA_ROOTDIR);

    _configBuilder = new SchemaRegistry.Config();
    _configBuilder.setMode("file");
    _configBuilder.setRootSchemaNamespace(SCHEMA_ROOTDIR);

    _config = _configBuilder.build();
  }

  @Test
  public void testFetchLatestSchemaBySourceId() throws InvalidConfigException, DatabusException,
                                                   NoSuchSchemaException
  {
    SourceIdNameRegistry sourceIdNameRegistry = new SourceIdNameRegistry();

    EspressoBackedSchemaRegistryService service =
        new EspressoBackedSchemaRegistryService(_config, sourceIdNameRegistry);

    EspressoSchemaName schemaName =
      EspressoSchemaName.create("BizFollow","BizFollowData");

    SchemaId schemaId = service.fetchSchemaIdForSourceNameAndVersion(schemaName.getDatabusSourceName(), (short)1);
    Assert.assertEquals(schemaId.toString(), "63c61224ff4ef62172a5b6316ee04b");

    schemaName =
      EspressoSchemaName.create("EspressoDB", "IdNamePair");
    schemaId = service.fetchSchemaIdForSourceNameAndVersion(schemaName.getDatabusSourceName(), (short)2);
    Assert.assertEquals(schemaId.toString(), "24d58337b32bd94521192c8b6c97386");
    schemaId = service.fetchSchemaIdForSourceNameAndVersion(schemaName.getDatabusSourceName(), (short)1);
    Assert.assertEquals(schemaId.toString(), "f8d97bbf6528db8612f2723866ccc98");

    try
    {
      // Does not have this version
      schemaId = service.fetchSchemaIdForSourceNameAndVersion(schemaName.getDatabusSourceName(), (short)3);
      Assert.assertTrue(false);
    } catch (DatabusException de)
    {
      // Must throw
    }


    /**
     * DB is present, table is not
     */
    try
    {
      schemaName = EspressoSchemaName.create("BizFollow","NotPresentTable");
      schemaId = service.fetchSchemaIdForSourceNameAndVersion(schemaName.getDatabusSourceName(), (short)1);
      Assert.assertTrue(false);
    } catch (DatabusException de)
    {
      // Must throw
    }

    /**
     * DB is not present, table is
     */
    try
    {
      schemaName = EspressoSchemaName.create("NotPresentDB","IdNamePair");
      schemaId = service.fetchSchemaIdForSourceNameAndVersion(schemaName.getDatabusSourceName(), (short)1);
    } catch (DatabusException de)
    {
      // Must throw
    }
  }

  @Test
  public void testFetchLatestSchemaByType() throws InvalidConfigException, DatabusException,
                                                   NoSuchSchemaException
  {
    SourceIdNameRegistry sourceIdNameRegistry = new SourceIdNameRegistry();

    EspressoBackedSchemaRegistryService service =
        new EspressoBackedSchemaRegistryService(_config, sourceIdNameRegistry);

    EspressoSchemaName schemaName =
        EspressoSchemaName.create("BizFollow","BizFollowData");

    String bizfollowSchemaStr = service.fetchLatestSchemaByType(schemaName.getDatabusSourceName());
    Assert.assertNotNull(bizfollowSchemaStr);
    Schema bizfollowSchema = Schema.parse(bizfollowSchemaStr);
    Assert.assertNotNull(bizfollowSchema);
    Assert.assertEquals(bizfollowSchema.getName(), "BizFollowData");
    Assert.assertEquals(bizfollowSchema.getNamespace(), "com.linkedin.data.bizfollow");

    //make sure the logical sources ids are correct
    Assert.assertEquals(sourceIdNameRegistry.getSourceId(schemaName.getDatabusSourceName()).intValue(), 0);

    /**
     * DB is not present
     */
    schemaName = EspressoSchemaName.create("NotPresentDB","BizFollowData");

    try
    {
      bizfollowSchemaStr = service.fetchLatestSchemaByType(schemaName.getDatabusSourceName());
      Assert.assertFalse(true);
    } catch (NoSuchSchemaException nsse)
    {
      Assert.assertTrue(true);
    }

    /**
     * table is not present
     */
    try
    {
      schemaName = EspressoSchemaName.create("BizFollow","NotPresentTable");
      bizfollowSchemaStr = service.fetchLatestSchemaByType(schemaName.getDatabusSourceName());
      Assert.assertTrue(false);
    } catch (NoSuchSchemaException nsse)
    {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testFetchAllSchemaVersionsByType() throws InvalidConfigException, DatabusException,
                                                   NoSuchSchemaException
  {
    SourceIdNameRegistry sourceIdNameRegistry = new SourceIdNameRegistry();

    EspressoBackedSchemaRegistryService service =
        new EspressoBackedSchemaRegistryService(_config, sourceIdNameRegistry);

    EspressoSchemaName schemaName =
        EspressoSchemaName.create("EspressoDB", "IdNamePair");

    Map<Short, String> idnameSchemaStrs =
        service.fetchAllSchemaVersionsByType(schemaName.getDatabusSourceName());
    Assert.assertNotNull(idnameSchemaStrs);
    Assert.assertTrue(idnameSchemaStrs.size() >= 2);

    Schema v1Schema = Schema.parse(idnameSchemaStrs.get((short)1));
    Assert.assertNotNull(v1Schema);
    Assert.assertEquals(v1Schema.getName(), "IdNamePair");
    Assert.assertEquals(v1Schema.getNamespace(), "com.linkedin.espresso.test.espressodb");
    //Assert.assertEquals(v1Schema.getProp("version"), "1");
    Assert.assertNull(v1Schema.getField("address"));

    Schema v2Schema = Schema.parse(idnameSchemaStrs.get((short)2));
    Assert.assertNotNull(v2Schema);
    Assert.assertEquals(v2Schema.getName(), "IdNamePair");
    Assert.assertEquals(v2Schema.getNamespace(), "com.linkedin.espresso.test.espressodb");
    //Assert.assertEquals(v2Schema.getProp("version"), "2");
    Assert.assertNotNull(v2Schema.getField("address"));

    //make sure the logical sources ids are correct - numbers start with 0

    EspressoSchemaName emailSchemaName =
        EspressoSchemaName.create("EspressoDB", "Email");

    Assert.assertEquals(sourceIdNameRegistry.getSourceId(emailSchemaName.getDatabusSourceName()).intValue(), 16);
    EspressoSchemaName emailTestSchemaName =
        EspressoSchemaName.create("EspressoDB", "EmailTest");
    Assert.assertEquals(sourceIdNameRegistry.getSourceId(emailTestSchemaName.getDatabusSourceName()).intValue(), 17);
    EspressoSchemaName edataSchemaName =
        EspressoSchemaName.create("EspressoDB", "EspressoData");
    Assert.assertEquals(sourceIdNameRegistry.getSourceId(edataSchemaName.getDatabusSourceName()).intValue(), 18);
    EspressoSchemaName addressSchemaName =
        EspressoSchemaName.create("EspressoDB", "IdAddressPair");
    Assert.assertEquals(sourceIdNameRegistry.getSourceId(addressSchemaName.getDatabusSourceName()).intValue(), 19);
    EspressoSchemaName nameSchemaName =
        EspressoSchemaName.create("EspressoDB", "IdNamePair");
    Assert.assertEquals(sourceIdNameRegistry.getSourceId(nameSchemaName.getDatabusSourceName()).intValue(), 20);
  }

  @Test
  public void testFetchAllSchemaVersionsByType1()
  throws InvalidConfigException, DatabusException, NoSuchSchemaException
  {
    SourceIdNameRegistry sourceIdNameRegistry = new SourceIdNameRegistry();

    EspressoBackedSchemaRegistryService service =
        new EspressoBackedSchemaRegistryService(_config, sourceIdNameRegistry);

    EspressoSchemaName schemaName =
        EspressoSchemaName.create("BizFollow","BizFollowData");

    Map<Short,String> versionedSchemas = service.fetchAllSchemaVersionsByType(schemaName.getDatabusSourceName());
    Assert.assertNotNull(versionedSchemas);
    Assert.assertEquals(versionedSchemas.size(), 1);
    // By virtue of the fetchAllSchemaVersionsByType call, we expect SourceIdNameRegistry to get populated
    Assert.assertEquals(sourceIdNameRegistry.getSourceId(schemaName.getDatabusSourceName()).intValue(), 0);

    schemaName = EspressoSchemaName.create("EspressoDB","IdNamePair");

    // By virtue of the fetchAllSchemaVersionsByType call, we expect SourceIdNameRegistry to get populated, but not for another database
    Assert.assertNull(sourceIdNameRegistry.getSourceId(schemaName.getDatabusSourceName()));

    versionedSchemas = service.fetchAllSchemaVersionsByType(schemaName.getDatabusSourceName());
    Assert.assertNotNull(versionedSchemas);
    Assert.assertEquals(versionedSchemas.size(), 2);

    schemaName = EspressoSchemaName.create("NotPresentDB","NotPresentIdNamePair");
    versionedSchemas = service.fetchAllSchemaVersionsByType(schemaName.getDatabusSourceName());
    Assert.assertNotNull(versionedSchemas);
    Assert.assertEquals(versionedSchemas.size(), 0);
   }

  @Test
  public void testFetchSchemaIdForSourceNameAndVersions1()
  throws InvalidConfigException, DatabusException, NoSuchSchemaException
  {
    SourceIdNameRegistry sourceIdNameRegistry = new SourceIdNameRegistry();

    EspressoBackedSchemaRegistryService service =
        new EspressoBackedSchemaRegistryService(_config, sourceIdNameRegistry);

    /**
     * DB is present, table is present ( in schemas registry )
     */
    EspressoSchemaName schemaName =
        EspressoSchemaName.create("BizFollow","BizFollowData");

    SchemaId schemaId = service.fetchSchemaIdForSourceNameAndVersion(schemaName.getDatabusSourceName(), 1);
    Assert.assertNotNull(schemaId);

    schemaName = EspressoSchemaName.create("EspressoDB","IdNamePair");
    schemaId = service.fetchSchemaIdForSourceNameAndVersion(schemaName.getDatabusSourceName(), 2);
    Assert.assertNotNull(schemaId);

    /**
     * DB is not present, table is not present
     */
    schemaName = EspressoSchemaName.create("NotPresentDB","NotPresentIdNamePair");
    try
    {
    	schemaId = service.fetchSchemaIdForSourceNameAndVersion(schemaName.getDatabusSourceName(), 2);
    	// Must not come here.
        Assert.assertFalse(true);
    } catch (DatabusException de)
    {
      // Expect the exception
      Assert.assertTrue(true);
    }

    /**
     * DB is present, table is not present
     */
    schemaName = EspressoSchemaName.create("EspressoDB","NotPresentIdNamePair");
    try
    {
        schemaId = service.fetchSchemaIdForSourceNameAndVersion(schemaName.getDatabusSourceName(), 2);
        // Must not come here.
        Assert.assertFalse(true);
    } catch (DatabusException de)
    {
      // Expect the exception
      Assert.assertTrue(true);
    }

    /**
     * DB is not present, table is present
     */
    schemaName = EspressoSchemaName.create("NotPresentEspressoDB","IdNamePair");
    try
    {
        schemaId = service.fetchSchemaIdForSourceNameAndVersion(schemaName.getDatabusSourceName(), 2);
        // Must not come here.
        Assert.assertFalse(true);
    } catch (DatabusException de)
    {
      // Expect the exception
      Assert.assertTrue(true);
    }
   }

  @Test
  public void testFetchLatestSchemaObjByType()
  throws InvalidConfigException, DatabusException, NoSuchSchemaException
  {
    SourceIdNameRegistry sourceIdNameRegistry = new SourceIdNameRegistry();

    EspressoBackedSchemaRegistryService service =
        new EspressoBackedSchemaRegistryService(_config, sourceIdNameRegistry);

    EspressoSchemaName schemaName =
        EspressoSchemaName.create("BizFollow","BizFollowData");

    SchemaId sid1 = service.fetchSchemaIdForSourceNameAndVersion(schemaName.getDatabusSourceName(), 1);
    Schema schema = service.fetchLatestSchemaObjByType(schemaName.getDatabusSourceName());
    Assert.assertNotNull(schema);
    Assert.assertEquals(SchemaId.forSchema(schema), sid1);

    schemaName = EspressoSchemaName.create("EspressoDB","IdNamePair");
    schema = service.fetchLatestSchemaObjByType(schemaName.getDatabusSourceName());
    sid1 = service.fetchSchemaIdForSourceNameAndVersion(schemaName.getDatabusSourceName(), 2);
    Assert.assertEquals(SchemaId.forSchema(schema), sid1);

    schemaName = EspressoSchemaName.create("NotPresentDB","NotPresentIdNamePair");
    try
    {
        schema = service.fetchLatestSchemaObjByType(schemaName.getDatabusSourceName());
        Assert.assertTrue(false);
    }
    catch (NoSuchSchemaException nsse) {
      Assert.assertTrue(true);
    }
    catch (DatabusException nse){
    	// We should not get exception, as SchemaNotFoundException is eaten away as non-fatal
        Assert.assertTrue(false);
    }

   }
  
  @Test
  public void testDropDatabase1()
  throws InvalidConfigException, DatabusException, NoSuchSchemaException, NoSuchFieldException, IllegalAccessException
  {
    SourceIdNameRegistry sourceIdNameRegistry = new SourceIdNameRegistry();

    EspressoBackedSchemaRegistryService service =
        new EspressoBackedSchemaRegistryService(_config, sourceIdNameRegistry);

    Set<String> dbNames = Collections.synchronizedSet(new HashSet<String>());
	Field f = service.getClass().getDeclaredField("_knownDatabases");
	f.setAccessible(true);	  	  
	f.set(service, dbNames);

    service.dropDatabase("NotPresentDB");
    Assert.assertEquals(dbNames.size(), 0);

    dbNames.add("ucpx");
    dbNames.add("ucp");
    Assert.assertEquals(dbNames.size(), 2);
    
    service.dropDatabase("ucp");
    Assert.assertEquals(dbNames.size(), 1);
    
   }

  @Test
  public void testDropDatabase2()
  throws InvalidConfigException, DatabusException, NoSuchSchemaException, NoSuchFieldException, IllegalAccessException
  {
    SourceIdNameRegistry sourceIdNameRegistry = new SourceIdNameRegistry();

    EspressoBackedSchemaRegistryService service =
        new EspressoBackedSchemaRegistryService(_config, sourceIdNameRegistry);

    Set<String> dbNames = Collections.synchronizedSet(new HashSet<String>());
	Field f = service.getClass().getDeclaredField("_knownDatabases");
	f.setAccessible(true);	  	  
	f.set(service, dbNames);

    dbNames.add("ucpx");

    Map<VersionedTableSchemaLookup, SchemaId> logicalSrcName2SchemaId =
  		  new ConcurrentHashMap<VersionedTableSchemaLookup, SchemaId>();

	Field f2 = service.getClass().getDeclaredField("_logicalSrcName2SchemaId");
	f2.setAccessible(true);	  	  
	f2.set(service, logicalSrcName2SchemaId);

    service.dropDatabase("NotPresentDB");
    Assert.assertEquals(logicalSrcName2SchemaId.size(), 0);
    
    VersionedTableSchemaLookup vts = new VersionedTableSchemaLookup("ucpx.ActivityEvent", 1);
    String schema = "TestTestTestTest";
    SchemaId sid = new SchemaId(schema.getBytes());
    logicalSrcName2SchemaId.put(vts, sid);
    
    Assert.assertEquals(logicalSrcName2SchemaId.size(), 1);
    service.dropDatabase("ucpx");
    Assert.assertEquals(logicalSrcName2SchemaId.size(), 0);
    
    logicalSrcName2SchemaId.put(vts, sid);
    VersionedTableSchemaLookup vts2 = new VersionedTableSchemaLookup("ucpx.CheckSumTable", 2);
    String schema2 = "Test2Test2Test22";
    SchemaId sid2 = new SchemaId(schema2.getBytes());
    logicalSrcName2SchemaId.put(vts2, sid2);
    
    dbNames.add("ucpx");
    Assert.assertEquals(logicalSrcName2SchemaId.size(), 2);
    service.dropDatabase("ucpx");
    Assert.assertEquals(logicalSrcName2SchemaId.size(), 0);

    
   }

}
