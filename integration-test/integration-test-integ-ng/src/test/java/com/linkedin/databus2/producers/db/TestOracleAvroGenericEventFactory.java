package com.linkedin.databus2.producers.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBuffer.AllocationPolicy;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.producers.ConstantPartitionFunction;
import com.linkedin.databus2.producers.EventCreationException;
import com.linkedin.databus2.schemas.FileSystemSchemaRegistryService;
import com.linkedin.databus2.schemas.NoSuchSchemaException;

public class TestOracleAvroGenericEventFactory
{
  public static final Logger LOG = Logger.getLogger(TestOracleAvroGenericEventFactory.class.getName());
  volatile String            _dbHostname;
  volatile String            _schemaRegistryPath;
  volatile FileSystemSchemaRegistryService _schemaReg;

  @BeforeClass
  public void setUp() throws ClassNotFoundException, InvalidConfigException
  {
    Logger.getRootLogger().setLevel(Level.ERROR);
    Logger.getRootLogger().setLevel(Level.INFO);
    ClassLoader.getSystemClassLoader().loadClass("oracle.jdbc.driver.OracleDriver");
    _dbHostname = System.getProperty("db.hostname", "localhost");
    _schemaRegistryPath = System.getProperty("schema.registry.path", "schemas_registry");

    LOG.info("using db.hostname=" + _dbHostname);
    LOG.info("using schema.registry.path=" + _schemaRegistryPath);

    FileSystemSchemaRegistryService.Config _schemaRegConfBuilder =
       new FileSystemSchemaRegistryService.Config();

    _schemaRegConfBuilder.setSchemaDir(_schemaRegistryPath);
    _schemaRegConfBuilder.setRefreshPeriodMs(-1);
    _schemaReg = FileSystemSchemaRegistryService.build(_schemaRegConfBuilder.build());
  }

  @AfterClass
  public void tearDown()
  {
    if (null != _schemaReg) _schemaReg.stopSchemasRefreshThread();
  }

  public Connection connectToDB(String user, String passwd) throws SQLException
  {
    Connection result =
        DriverManager.getConnection("jdbc:oracle:thin:" + user + "/" + passwd + "@"
            + _dbHostname + ":1521:DB");

    return result;

  }

  @Test
  public void testAnet() throws SQLException, EventCreationException, UnsupportedKeyException,
                                NoSuchSchemaException, DatabusException, IOException
  {

    String eventSchema = _schemaReg.fetchLatestSchemaByType("com.linkedin.events.anet.Anets");
    Assert.assertNotNull(eventSchema);
    Assert.assertTrue(eventSchema.length() > 0);

    Connection conn = null;

    Statement stmt = null;
    PreparedStatement insStmt = null;
    PreparedStatement insSettingsStmt = null;
    PreparedStatement eventStmt = null;

    try
    {
      conn = connectToDB("anet", "anet");
      conn.setAutoCommit(false);

      stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT MAX(anet_id) from ANETS ");

      //get the current state of the DB - max anet_id and max scn
      int maxAnetId = 0;
      if (rs.next()) maxAnetId = rs.getInt(1);
      int newAnetId = maxAnetId + 1;
      rs.close();
      LOG.info("adding anet record with anet_id=" + newAnetId);

      rs = stmt.executeQuery("SELECT MAX(scn) FROM sy$txlog WHERE scn < 9999999999999");
      long maxScn = 0;
      if (rs.next()) maxScn = rs.getInt(1);
      rs.close();
      LOG.info("existing scn=" + maxScn);

      //add a new anets record
      insStmt =
          conn.prepareStatement("INSERT INTO anets(anet_id, anet_type, entity_id, state, name, locale, created_at, updated_at, url_key, owner_id, country, postal_code, geo_postal_code, region_code, geo_place_code, geo_place_mask_code, latitude_deg, longitude_deg, anet_size, parent_anet_id, contact_email, short_description) VALUES(?, 'UGRP', ?, 'A', ?, 'US', SYSDATE, SYSDATE, ?, 1, 'US', 99999, 99999, 1, 2, NULL, 4, 5.5, 666, 1, 'my@email.com', 'my test anet')");
      insStmt.setInt(1, newAnetId);
      insStmt.setInt(2, newAnetId);
      insStmt.setString(3, "group" + newAnetId);
      insStmt.setString(4, "url" + newAnetId);
      Assert.assertTrue(!insStmt.execute());
      LOG.info("anets row added");

      //add associated settigns for the new anet
      insSettingsStmt =
          conn.prepareStatement("INSERT ALL INTO anet_settings(anet_id, setting_id, setting_value, date_created, date_modified) VALUES(?, 1, 'SET1', SYSDATE, SYSDATE) INTO anet_settings(anet_id, setting_id, setting_value, date_created, date_modified) VALUES(?, 2, 'SET2', SYSDATE, SYSDATE) INTO anet_settings(anet_id, setting_id, setting_value, date_created, date_modified)  VALUES(?, 3, 'SET3', SYSDATE, SYSDATE) SELECT * FROM dual");
      insSettingsStmt.setInt(1, newAnetId);
      insSettingsStmt.setInt(2, newAnetId);
      insSettingsStmt.setInt(3, newAnetId);

      Assert.assertTrue(!insSettingsStmt.execute());
      LOG.info("anet_settings rows added");

      conn.commit();
      LOG.info("xion committed");

      //create an event factory to read the event from the db
      OracleAvroGenericEventFactory eventFactory =
          new OracleAvroGenericEventFactory((short)5, (short)10, eventSchema,
                                            new ConstantPartitionFunction((short)10));
      MonitoredSourceInfo msInfo =
          new MonitoredSourceInfo((short)5, "anet", "com.linkedin.events.anet.Anets", "anets_5",
                                  eventFactory, null, false);

      //run the query to read recent events
      String eventsQry = OracleTxlogEventReader.generateEventQuery(msInfo, "");
      LOG.info("running query: " + eventsQry);

      eventStmt = conn.prepareStatement(eventsQry);
      eventStmt.setLong(1, maxScn);
      eventStmt.setLong(2, maxScn);

      rs = eventStmt.executeQuery();
      Assert.assertTrue(rs.next());
      LOG.info("read anet row");

      //allocate the buffer to store the anet event
      DbusEventBuffer.Config bufferConfBuilder = new DbusEventBuffer.Config();
      bufferConfBuilder.setAllocationPolicy(AllocationPolicy.HEAP_MEMORY.toString());
      bufferConfBuilder.setMaxSize(100000);
      bufferConfBuilder.setReadBufferSize(10000);
      bufferConfBuilder.setScnIndexSize(1000);

      DbusEventBuffer eventBuffer = new DbusEventBuffer(bufferConfBuilder);
      eventBuffer.start(1);

      //add a single window with the test event
      eventBuffer.startEvents();
      long currentTime = System.currentTimeMillis();
      long scn = rs.getLong(1);
      eventFactory.createAndAppendEvent(scn, currentTime, rs, eventBuffer, false, null);

      Assert.assertTrue(!rs.next());
      rs.close();
      conn.commit();
      LOG.info("xion committed");

      eventBuffer.endEvents(scn);

      //read and smoke-test the contents of the buffer
      DbusEventBuffer.DbusEventIterator eventIter = eventBuffer.acquireIterator("testIter");
      Assert.assertTrue(eventIter.hasNext());
      eventIter.next(); //skip over the initial marker eop event

      Assert.assertTrue(eventIter.hasNext());
      DbusEvent e = eventIter.next();

      Assert.assertEquals(e.sequence(), scn);
      Assert.assertEquals(e.timestampInNanos(), currentTime * 1000000);
      Assert.assertEquals(e.srcId(), (short)5);
      Assert.assertEquals(e.logicalPartitionId(), (short)10);

      //parse the avro value
      byte[] evalue = new byte[e.valueLength()];
      e.value().get(evalue);

      Schema schema = Schema.parse(eventSchema);
      GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
      BinaryDecoder binDecoder = DecoderFactory.defaultFactory().createBinaryDecoder(evalue, null);
      GenericRecord r = reader.read(null, binDecoder);

      Assert.assertEquals(r.get("anetId"), Long.valueOf(newAnetId));
      Assert.assertEquals(r.get("anetType").toString(), "UGRP");
      Assert.assertEquals(r.get("entityId"), Long.valueOf(newAnetId));
      Assert.assertEquals(r.get("state").toString(), "A");
      Assert.assertEquals(r.get("name").toString(), "group" + newAnetId);
      Assert.assertEquals(r.get("locale").toString().trim(), "US");
      Assert.assertNotNull(r.get("createdAt"));
      //Assert.assertEquals(r.get("createdAt"), r.get("updatedAt")); --> Need not be true, Two successive invocations of SYSDATE need not return the same values
      Assert.assertEquals(r.get("urlKey").toString(), "url" + newAnetId);
      Assert.assertEquals(r.get("ownerId"), Long.valueOf(1));
      Assert.assertEquals(r.get("anetSize"), Long.valueOf(666));
      Assert.assertEquals(r.get("parentAnetId"), Long.valueOf(1));
      Assert.assertEquals(r.get("contactEmail").toString(), "my@email.com");
      Assert.assertEquals(r.get("shortDescription").toString(), "my test anet");
      Assert.assertNull(r.get("description"));
      Assert.assertNull(r.get("largeLogoId"));
      Assert.assertNull(r.get("smallLogoId"));
      Assert.assertNull(r.get("vanityUrl"));

      GenericRecord geoData = (GenericRecord)r.get("geo");
      Assert.assertNotNull(geoData);
      Assert.assertEquals(geoData.get("country").toString().trim(), "US");
      Assert.assertEquals(geoData.get("postalCode").toString(), "99999");
      Assert.assertEquals(geoData.get("geoPostalCode").toString(), "99999");
      Assert.assertEquals(geoData.get("regionCode"), Integer.valueOf(1));
      Assert.assertEquals(geoData.get("geoPlaceCode").toString(), "2");
      Assert.assertNull(geoData.get("geoPlaceMaskCode"));
      Assert.assertEquals(geoData.get("latitudeDeg"), Float.valueOf(4));
      Assert.assertEquals(geoData.get("longitudeDeg"), Float.valueOf((float)5.5));

      GenericRecord settings = (GenericRecord)r.get("settings");
      Assert.assertNotNull(settings);
      List<?> settingsArr = (List<?>)settings.get("settings");
      Assert.assertNotNull(settingsArr);
      Assert.assertEquals(settingsArr.size(), 3);

      for (int i = 1; i <= 3; ++i)
      {
        GenericRecord item = (GenericRecord)settingsArr.get(i - 1);
        Assert.assertEquals(item.get("settingId"), Long.valueOf(i));
        Assert.assertEquals(item.get("settingValue").toString(), "SET" + i);
        Assert.assertNotNull(item.get("dateCreated"));
        Assert.assertEquals(item.get("dateCreated"), item.get("dateModified"));
      }
    }
    finally
    {
      if (null != eventStmt) eventStmt.close();
      if (null != insSettingsStmt) insSettingsStmt.close();
      if (null != insStmt) insStmt.close();
      if (null != stmt) stmt.close();
      if (null != conn) conn.close();
    }

  }

}
