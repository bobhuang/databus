package com.linkedin.databus3.espresso;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBufferMult;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.data_model.PhysicalSource;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.schemas.SourceIdNameRegistry;
import com.linkedin.databus3.espresso.EspressoRelay.StaticConfigBuilder;
import com.linkedin.databus3.espresso.cmclient.ClusterManagerPhysicalSourceConfigBuilder;
import com.linkedin.databus3.espresso.cmclient.ResourceManager;
import com.linkedin.databus3.espresso.cmclient.TestClusterManagerDynamicAddRemovePartitions;
import com.linkedin.databus3.espresso.schema.EspressoBackedSchemaRegistryService;
import com.linkedin.espresso.common.config.InvalidConfigException;
import com.linkedin.espresso.schema.SchemaRegistry;

@Test(singleThreaded=true)
public class TestEspressoRelayWithClusterManager
{
  private final static Logger LOG = Logger.getLogger(TestClusterManagerDynamicAddRemovePartitions.class);

  static
  {
    //PatternLayout defaultLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c} %m%n");
    PatternLayout defaultLayout = new PatternLayout("%m%n");
    ConsoleAppender defaultAppender = new ConsoleAppender(defaultLayout);

    Logger.getRootLogger().removeAllAppenders();
    Logger.getRootLogger().addAppender(defaultAppender);

    Logger.getRootLogger().setLevel(Level.OFF);
    Logger.getRootLogger().setLevel(Level.ERROR);


    //Logger.getRootLogger().setLevel(Level.INFO);
    //Logger.getRootLogger().setLevel(Level.DEBUG);
  }

  private EspressoRelay.StaticConfig _espressoRelayStaticConfig;
  private EspressoRelay _realRelay;
  private DbusEventBufferMult _bufMult;
  private int _relayPort;
  private EspressoBackedSchemaRegistryService _schemaRegistry;
  private SourceIdNameRegistry _sourceIdNameRegistry;
  private final ResourceManager _rm  = new ResourceManager();
  private String _mmapDir = "/tmp/mmap";

  private final static String DB_NAME = "BizProfile"; //because we have to use a real registry - we have to use a real name :(
  private final static String SCHEMA_ROOTDIR_PROP_NAME = "espresso.schema.rootdir";
  private final static String DEFAULT_ESPRESSO_SCHEMA_ROOTDIR = "../databus3-relay-cmdline-pkg/espresso_schemas_registry";
  private static String SCHEMA_ROOTDIR;

  @BeforeMethod
  public void setUp() throws InvalidConfigException, com.linkedin.databus.core.util.InvalidConfigException, DatabusException, IOException {
    SCHEMA_ROOTDIR = System.getProperty(SCHEMA_ROOTDIR_PROP_NAME, DEFAULT_ESPRESSO_SCHEMA_ROOTDIR);
    LOG.info("Using espresso schema registry root: " + SCHEMA_ROOTDIR);

    // create fake registries - we have to use a real espresso schema registry
    SchemaRegistry.Config configBuilder = new SchemaRegistry.Config();
    configBuilder.setMode("file");

    configBuilder.setRootSchemaNamespace(SCHEMA_ROOTDIR);

    SchemaRegistry.StaticConfig config = configBuilder.build();
    SourceIdNameRegistry sourceIdNameRegistry = new SourceIdNameRegistry();

    _schemaRegistry = new EspressoBackedSchemaRegistryService(config, sourceIdNameRegistry);
    List<IdNamePair> pairs = new ArrayList<IdNamePair>();
    long i = 0;
    for(String source :_schemaRegistry.getAllLogicalSourcesInDatabase(DB_NAME)) {
      pairs.add(new IdNamePair(i++, source));
    }
    _sourceIdNameRegistry = new SourceIdNameRegistry();
    _sourceIdNameRegistry.updateFromIdNamePairs(pairs);

    // create actuall real empty Espresso Relay
    EspressoRelay.StaticConfigBuilder cfgBuilder = new StaticConfigBuilder();
    cfgBuilder.getEventBuffer().setMaxSize(10*1024*1024);
    cfgBuilder.getEventBuffer().setScnIndexSize(2*1024*1024);
    //cfgBuilder.getEventBuffer().setMaxIndividualBufferSize(1024*1024);
    cfgBuilder.getEventBuffer().setAllocationPolicy("MMAPPED_MEMORY");
    cfgBuilder.getEventBuffer().setMmapDirectory(_mmapDir);
    cfgBuilder.getEventBuffer().getTrace().setOption("file");
    cfgBuilder.getEventBuffer().getTrace().setAppendOnly(false);
    cfgBuilder.getEventBuffer().setRestoreMMappedBuffers(true);

    _relayPort = 25001;
    cfgBuilder.getContainer().setHttpPort(_relayPort);
    cfgBuilder.getContainer().getJmx().setJmxServicePort(25002);
    cfgBuilder.getClusterManager().setInstanceName("localhost_10015");
    _espressoRelayStaticConfig = cfgBuilder.build();
    _realRelay = new EspressoRelay(_espressoRelayStaticConfig, null, _sourceIdNameRegistry, _schemaRegistry);
    _bufMult = _realRelay.getEventBuffer();

    // setup callback for resources
    ClusterManagerPhysicalSourceConfigBuilder cmPSCB =
        new ClusterManagerPhysicalSourceConfigBuilder(null, _sourceIdNameRegistry, _schemaRegistry, _realRelay);

    _rm.setListener(cmPSCB); // listener will be called on add/remove calls

    LOG.info("Before test completed. Buf = " + _bufMult + ";relay=" + _realRelay);
  }

  private boolean deleteDirectory(String fileName)
  {
	  File f = new File(fileName);
	  if (f.isDirectory())
	  {
		  String[] children = f.list();
		  for (int cnt=0; cnt < children.length;cnt++)
		  {
			  String fc = fileName + "/" + children[cnt];
			  boolean success = deleteDirectory(fc);
			  if (!success)
				  return false;
		  }
		  boolean success = f.delete();
		  if (!success)
			  return false;
	  }
	  else if (f.isFile())
	  {
		  boolean success = f.delete();
		  if (!success)
			  return false;
	  }
	  return true;	  
  }
  
  @Test(enabled=false)
  private void testDropDatabase() {
	    LOG.info("startig testClusterManagerMessages. Buf=" + _bufMult);
	    // Clear out contents in mmap directory
	    deleteDirectory(_mmapDir);
	    // create buffer -
	    final String uri1 = "ela4-db1-espresso.prod.linkedin.com_1521";
	    final String pName = DB_NAME;

	    List<String> rks = new ArrayList<String>(20);
	    int[] expectedState = new int[5];
	    Arrays.fill(expectedState, -1);

	    int partId = 1;
	    rks.add("add:" + uri1 + ","+pName+",p" + partId + "_1,MASTER");
	    expectedState[partId] = 1; // one ref count
	    playScenario(rks);
	    for (DbusEventBuffer dBuf : _bufMult.bufIterable())
	    {
//	    	dBuf.flushIterable(true);
	    }
	    //_bufMult.saveBufferMetaInfo(false);
	    
	    rks.remove(0);
		rks.add("dropDatabase:" + DB_NAME );
	    playScenario(rks);
	    
	    File f = new File(_mmapDir);
	    Assert.assertEquals(f.list().length, 0);
        
  }
  
  @Test
  public void testClusterManagerMessages() {
    LOG.info("startig testClusterManagerMessages. Buf=" + _bufMult);
    // create buffer -
    final String uri1 = "ela4-db1-espresso.prod.linkedin.com_1521";
    final String uri2 = "ela4-db1-espresso.prod.linkedin.com_1522";
    final String uri3 = "ela4-db1-espresso.prod.linkedin.com_1523";
    final String pName = DB_NAME;

    List<String> rks = new ArrayList<String>(20);
    int[] expectedState = new int[5];
    Arrays.fill(expectedState, -1);

    // add - means Offline=>Online
    // rm - means Online=>Offline
    int partId = 1;
    // add as a slave, upgrade to master - count 1
    rks.add("add:" + uri1 + ","+pName+",p" + partId + "_1,SLAVE");
    rks.add("rm:" + uri1 + ","+pName+",p" + partId + "_1,SLAVE");
    rks.add("add:" + uri1 + ","+pName+",p" + partId + "_1,MASTER");
    // expected result
    expectedState[partId] = 1; // one ref count
    //playScenario(rks);


    partId = 2;
    // add slave, upgrade to master - count = 1
    rks.add("add:" + uri1 + ","+pName+",p" + partId + "_1,SLAVE");
    rks.add("rm:" + uri1 + ","+pName+",p" + partId + "_1,SLAVE");
    rks.add("add:" + uri1 + ","+pName+",p" + partId + "_1,MASTER");
    // add 2 more slaves
    rks.add("add:" + uri2 + ","+pName+",p" + partId + "_1,SLAVE");
    rks.add("add:" + uri3 + ","+pName+",p" + partId + "_1,SLAVE");
    expectedState[partId] = 3; // master and slave




    partId = 3;
    // added as a slave - count = 1
    rks.add("add:" + uri1 + ","+pName+",p" + partId + "_1,SLAVE");
    // converted to master - count = 1
    rks.add("rm:" + uri1 + ","+pName+",p" + partId + "_1,SLAVE");
    rks.add("add:" + uri1 + ","+pName+",p" + partId + "_1,MASTER");
    // master fails - count = 0;
    rks.add("rm:" + uri1 + ","+pName+",p" + partId + "_1,MASTER");
    // adding as a slave and then upgrading to master - count = 1
    rks.add("add:" + uri2 + ","+pName+",p" + partId + "_1,SLAVE");
    rks.add("rm:" + uri2 + ","+pName+",p" + partId + "_1,SLAVE");
    rks.add("add:" + uri2 + ","+pName+",p" + partId + "_1,MASTER");
    // adding one more slave - count = 2
    rks.add("add:" + uri2 + ","+pName+",p" + partId + "_1,SLAVE");
    // adding one more slave - count = 3
    rks.add("add:" + uri3 + ","+pName+",p" + partId + "_1,SLAVE");
    // first slave fails - count = 2
    rks.add("rm:" + uri2 + ","+pName+",p" + partId + "_1,SLAVE");
    expectedState[partId] = 2; // one master (uri2) and one slave (uri3)

    playScenario(rks);

    // in-between validation
    partId = 2;
    PhysicalPartition pPart = new PhysicalPartition(partId, pName);
    DbusEventBuffer buf1 = _bufMult.getOneBuffer(pPart);
    Assert.assertNotNull(buf1);
    DbusEventBuffer buf2 = _bufMult.getOneBuffer(pPart);
    Assert.assertNotNull(buf2);
    DbusEventBuffer buf3 = _bufMult.getOneBuffer(pPart);
    Assert.assertNotNull(buf3);
    Assert.assertTrue(buf1 == buf2, " should be same buffer");
    Assert.assertTrue(buf1 == buf3, " should be same buffer");

    // verify
    for(int i=0; i<expectedState.length; i++) {
      if(expectedState[i] == -1)
        continue; //nothing expected

      pPart = new PhysicalPartition(i, pName);
      Set<PhysicalSource> set = _bufMult.getPhysicalSourcesForPartition(pPart);
      Assert.assertNotNull(set);
      System.out.println("set for p = " + pPart  + ":" + Arrays.toString(set.toArray()));
      Assert.assertEquals(set.size(), expectedState[i]);
      DbusEventBuffer buf = _bufMult.getOneBuffer(pPart);
      Assert.assertEquals(buf.getRefCount(), expectedState[i], "buf for part = " + pPart);
    }

  }

  // for each buffer (physical partition ) we create a mapping of
  // corresponding physicalSources (and ResourceKeys)
  // validate that we can add/remove them correctly
  @Test
  public void testPhysicalSourceMapping() {
    LOG.info("startig testPhysicalSourceMapping. Buf=" + _bufMult);
    Logger logger = Logger.getLogger(DbusEventBufferMult.class.getName());
    final String uri1 = "ela4-db1-espresso.prod.linkedin.com_1521";
    final String uri2 = "ela4-db1-espresso.prod.linkedin.com_1522";
    final String uri3 = "ela4-db1-espresso.prod.linkedin.com_1523";
    final String pName = DB_NAME;

    List<String> rks = new ArrayList<String>(20);
    Set<PhysicalSource> verifySet = new HashSet<PhysicalSource>(3);

    // add - means Offline=>Online
    // rm - means Online=>Offline
    int partId = 1;
    // add as a slave and a master (different machine)
    rks.add("add:" + uri1 + ","+pName+",p" + partId + "_1,SLAVE");
    rks.add("add:" + uri2 + ","+pName+",p" + partId + "_1,MASTER");
    rks.add("add:" + uri3 + ","+pName+",p" + partId + "_1,SLAVE");
    playScenario(rks);

    // expected result - 3 physical partition for the same buffer
    verifySet.add(new PhysicalSource(uri1, PhysicalSource.PHYSICAL_SOURCE_SLAVE, ""));
    verifySet.add(new PhysicalSource(uri2, PhysicalSource.PHYSICAL_SOURCE_MASTER, ""));
    verifySet.add(new PhysicalSource(uri3, PhysicalSource.PHYSICAL_SOURCE_SLAVE, ""));

    validatePhysicalSourceMapping(1, pName, verifySet);

    // same test with different pPartId
    rks.clear();
    verifySet.clear();
    partId = 2;
    // add as a slave and a master (different machine)
    rks.add("add:" + uri1 + ","+pName+",p" + partId + "_1,SLAVE");
    rks.add("add:" + uri2 + ","+pName+",p" + partId + "_1,MASTER");
    rks.add("add:" + uri3 + ","+pName+",p" + partId + "_1,SLAVE");
    playScenario(rks);

    // expected result - 3 physical partition for the same buffer
    verifySet.add(new PhysicalSource(uri1, PhysicalSource.PHYSICAL_SOURCE_SLAVE, ""));
    verifySet.add(new PhysicalSource(uri2, PhysicalSource.PHYSICAL_SOURCE_MASTER, ""));
    verifySet.add(new PhysicalSource(uri3, PhysicalSource.PHYSICAL_SOURCE_SLAVE, ""));
    validatePhysicalSourceMapping(partId, pName, verifySet);

    // now remove some physical sources
    rks.clear();
    verifySet.clear();
    partId = 1;
    rks.add("rm:" + uri1 + ","+pName+",p" + partId + "_1,SLAVE");
    partId = 2;
    rks.add("rm:" + uri2 + ","+pName+",p" + partId + "_1,MASTER");
    playScenario(rks);

    // expected result - 2physical partition (uri2, uri3) for partId = 1
    partId = 1;
    verifySet.clear();
    verifySet.add(new PhysicalSource(uri2, PhysicalSource.PHYSICAL_SOURCE_MASTER, ""));
    verifySet.add(new PhysicalSource(uri3, PhysicalSource.PHYSICAL_SOURCE_SLAVE, ""));
    validatePhysicalSourceMapping(partId, pName, verifySet);


    // expected result - 2physical partition (uri1, uri3) for partId = 2
    partId = 2;
    verifySet.clear();
    verifySet.add(new PhysicalSource(uri1, PhysicalSource.PHYSICAL_SOURCE_SLAVE, ""));
    verifySet.add(new PhysicalSource(uri3, PhysicalSource.PHYSICAL_SOURCE_SLAVE, ""));
    validatePhysicalSourceMapping(partId, pName, verifySet);

    // test the http interface
    // expecting the following partitionId => num_of_phSource mapping
    // 0=>1 // default partition
    // 1=>2
    // 2=>2
    testHttpInterface(3, new int[] {1,2,2});
  }

  // just validates that there is a corresponding PhysicalSource int the buffer for
  // each uri (
  private void validatePhysicalSourceMapping(int partId, String pName, Set<PhysicalSource> pss) {
    PhysicalPartition pPart = new PhysicalPartition(partId, pName);
    Set<PhysicalSource> set = _bufMult.getPhysicalSourcesForPartition(pPart);
    Assert.assertNotNull(set);
    System.out.println("list for p = " + pPart  + ":" + Arrays.toString(set.toArray()));
    Assert.assertEquals(set.size(), pss.size());
    for(PhysicalSource ps: pss) {
      Assert.assertTrue(set.contains(ps));
    }
  }

  private void playScenario(final List<String> rks) {
    // run it as a separate thread (~ initiated from ZK client)
    Thread t = new Thread() {
      @Override
      public void run() {
        for(String rk : rks) {
          String [] split = rk.split(":");
          String cmd = split[0];
          rk = split[1];
          try {
        	if (cmd.equals("dropDatabase")) 
        		_rm.dropDatabase(rk);
        	else if(cmd.equals("add"))
              _rm.addResource(rk);
            else // "rm"
              _rm.removeResource(rk);
          } catch (Exception e) {
            Assert.fail(cmd + "faild with ", e);
          }
          LOG.info("play: " + cmd + "ed buf for rk=" + rk);
        }
      }
    };

    t.start();
    try
    {
      t.join(10000);
    }
    catch (InterruptedException e)
    {
      Assert.fail("Thread generating CM events timed out");
    }
  }


  @Test
  public void addRemoveRealRelayUnit() throws Exception {
    //not too many buffers to allow run as unit test
    addRemoveRealRelay(10);
  }

  // this test shouldn't be run as a unit test - it will run out of memory
  // should be used as manual testing
  private void addRemoveRealRelayLoad() throws Exception {
    // add a lot of buffers - out of memory if run with default settings
    addRemoveRealRelay(50);
  }

  // adds/removes buffers to/from relay using separate thread, starting from an empty relay
  private void addRemoveRealRelay(int bufNumIn) {
    final int bufNum = bufNumIn;

    final String uri = "ela4-db1-espresso.prod.linkedin.com_1521";
    final String pName = DB_NAME;
    //String rk;

    Thread t = new Thread() {
      @Override
      public void run() {
        String rk;
        for(int i=1; i<=bufNum; i++) {
          rk = uri + ","+pName+",p" + i + "_1,MASTER";
          try {
            _rm.addResource(rk);
          } catch (Exception e) {
            org.testng.Assert.fail();
          }
          LOG.info("added buf for rk=" + rk);
        }

        for(int i=1; i<=bufNum; i++) {
          PhysicalPartition pPart = new PhysicalPartition(i, pName);
          DbusEventBuffer buf = _bufMult.getOneBuffer(pPart);
          Assert.assertNotNull(buf);
        }
        for(int i=1; i<=bufNum; i+=2) {
          rk = uri + ","+pName+",p" + i + "_1,MASTER";
          try {
            _rm.removeResource(rk);
          } catch (Exception e) {
            Assert.fail("remove of rk=" + rk + " failed", e);
          }
          LOG.info("removing buf for rk=" + rk);
        }
      }
    };

    t.start();
    try
    {
      t.join(10000);
    }
    catch (InterruptedException e)
    {
      Assert.fail("Thread generating CM events timed out");
    }

    for(int i=1; i<=bufNum; i++) {
      PhysicalPartition pPart = new PhysicalPartition(i, pName);
      DbusEventBuffer buf = _bufMult.getOneBuffer(pPart);
      if(i%2 != 0)
        Assert.assertTrue(buf.shouldBeRemoved(true)); // ignore waiting time
      else
        Assert.assertNotNull(buf);
    }
  }

  // connect thru http - get all the buffers and corresponding physical sources
  private void testHttpInterface(int numOfBufs, int [] expectedCounts) {
    _realRelay.start();
    try
    {
      Thread.sleep(1000);
    }
    catch (InterruptedException e1) { }

    // connect and read
    String url = "http://localhost:"+_relayPort + "/physicalBuffers";
    String response = "";
    try
    {
      response = readHttpRequest(url);
    }
    catch (IOException e)
    {
      Assert.fail("failed to connect to relay over http", e);
    }

    // now convert response from json string to map
    ObjectMapper mapper = new ObjectMapper();
    Map<String, List<PhysicalSource>> map =
        new HashMap<String, List<PhysicalSource>>();
    try
    {
      map = mapper.readValue(new ByteArrayInputStream(response.getBytes()), map.getClass());
      Assert.assertEquals(map.size(), numOfBufs);

      // now validate the objects (just the size of the list of pSources per pPartId
      for(Map.Entry<String, List<PhysicalSource>> e : map.entrySet()) {
        String stringKey = e.getKey();
        List<PhysicalSource> l = e.getValue();
        System.out.println("pPart=" + stringKey + ": " + l);
        PhysicalPartition pPart = mapper.readValue(stringKey, PhysicalPartition.Builder.class).build();

        Assert.assertEquals(l.size(), expectedCounts[pPart.getId()]); // just the sizes
      }

    }
    catch (JsonParseException e)
    {
      Assert.fail("failed to parse response: " + response, e);
    }
    catch (JsonMappingException e)
    {
      Assert.fail("failed to parse response: " + response, e);
    }
    catch (IOException e)
    {
      Assert.fail("failed to parse response: " + response, e);
    }

    _realRelay.shutdown();
  }
  private String readHttpRequest(String urlString) throws IOException {
      URL url = new URL(urlString);
      URLConnection uc = url.openConnection();
      BufferedReader in = new BufferedReader(
                              new InputStreamReader(
                              uc.getInputStream()));
      String inputLine = in.readLine();
      in.close();
      if(inputLine == null)
        Assert.fail("got null result from relay");


      return inputLine;
  }
}
