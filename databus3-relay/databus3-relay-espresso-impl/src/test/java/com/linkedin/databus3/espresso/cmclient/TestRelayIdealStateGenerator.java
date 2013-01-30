package com.linkedin.databus3.espresso.cmclient;
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


import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.easymock.classextension.EasyMock;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.IdealState;

public class TestRelayIdealStateGenerator
{
	  public static final String MODULE = TestRelayIdealStateGenerator.class.getName();
	  public static final Logger LOG = Logger.getLogger(MODULE);

	  @Test
	  public void testEmptyExternalViewAndEmptyIdealState()
	  throws Exception
	  {
		  RelayIdealStateGenerator risg = new RelayIdealStateGenerator();
		  ZNRecord zr = risg.computeFuncISandEVofStorageCluster(null, "db");
		  Assert.assertEquals(zr.getId(),"db");
		  Assert.assertEquals(zr.getMapFields().size(), 0);
	  }

	  @Test
	  public void testEmptyExternalView()
	  throws Exception
	  {
		  RelayIdealStateGenerator risg = new RelayIdealStateGenerator();

		  String is =
				          "{" +
						  "  \"id\" : \"db\"," +
				          "  \"simpleFields\" : {" +
						  "  \"IDEAL_STATE_MODE\" : \"AUTO\"" +
						  "   }," +
						  "  \"mapFields\" : {" +
						  "    \"ucp_0\" : {" +
						  "      \"eat1-app33.stg.linkedin.com_11140\" : \"MASTER\"" +
						  "    }," +
						  "    \"ucp_1\" : {" +
						  "      \"eat1-app33.stg.linkedin.com_11140\" : \"SLAVE\"" +
						  "    }" +
						  "  }," +
						  "  \"listFields\" : {" +
						  "  \"ucp_0\" : [\"eat1-app33.stg.linkedin.com_11140\"]," +
						  "  \"ucp_1\" : [\"eat1-app33.stg.linkedin.com_11140\"]" +
				     	  "  }" +
                 		  "}";

		  ZNRecordSerializer zs = new ZNRecordSerializer();
		  ZNRecord zr = (ZNRecord) zs.deserialize(is.getBytes());

          StorageAdapter fakeAdapter = EasyMock.createMock(StorageAdapter.class);
          org.easymock.EasyMock.expect(fakeAdapter.getIdealState("db")).andReturn(zr).anyTimes();
          EasyMock.replay(fakeAdapter);

          Class[] args = new Class[2];
		  args[0] = ZNRecord.class;
		  args[1] = String.class;
		  Method m = risg.getClass().getDeclaredMethod("computeFuncISandEVofStorageCluster", args);
		  m.setAccessible(true);
		  Field f = risg.getClass().getDeclaredField("_storageAdapter");
		  f.setAccessible(true);
		  f.set(risg, fakeAdapter);

		  ZNRecord zr2 = risg.computeFuncISandEVofStorageCluster(null, "db");
		  Assert.assertEquals(zr2.getId(),"db");
		  Assert.assertEquals(zr2.getMapField("ucp_0").get("eat1-app33.stg.linkedin.com_11140"),"OFFLINE");
		  Assert.assertEquals(zr2.getMapField("ucp_1").get("eat1-app33.stg.linkedin.com_11140"),"OFFLINE");
	  }

	  @Test
	  public void testNonEmptyExternalView()
	  throws Exception
	  {
		  RelayIdealStateGenerator risg = new RelayIdealStateGenerator();

		  String is =
		          "{" +
				  "  \"id\" : \"db\"," +
		          "  \"simpleFields\" : {" +
				  "  \"IDEAL_STATE_MODE\" : \"AUTO\"" +
				  "   }," +
				  "  \"mapFields\" : {" +
				  "    \"ucp_0\" : {" +
				  "      \"eat1-app33.stg.linkedin.com_11140\" : \"MASTER\"" +
				  "    }," +
				  "    \"ucp_1\" : {" +
				  "      \"eat1-app33.stg.linkedin.com_11140\" : \"SLAVE\"" +
				  "    }" +
				  "  }," +
				  "  \"listFields\" : {" +
				  "  \"ucp_0\" : [\"eat1-app33.stg.linkedin.com_11140\"]," +
				  "  \"ucp_1\" : [\"eat1-app33.stg.linkedin.com_11140\"]" +
		     	  "  }" +
         		  "}";

		  ZNRecordSerializer zs = new ZNRecordSerializer();
		  ZNRecord zr = (ZNRecord) zs.deserialize(is.getBytes());

		  String ev =
		          "{" +
				  "  \"id\" : \"db\"," +
				  "  \"mapFields\" : {" +
				  "    \"ucp_0\" : {" +
				  "      \"eat1-app33.stg.linkedin.com_11140\" : \"MASTER\"" +
				  "    }" +
				  "  }" +
				  "}";
		  ZNRecord zev = (ZNRecord) zs.deserialize(ev.getBytes());

          StorageAdapter fakeAdapter = EasyMock.createMock(StorageAdapter.class);
          org.easymock.EasyMock.expect(fakeAdapter.getIdealState("db")).andReturn(zr).anyTimes();
          EasyMock.replay(fakeAdapter);

          Class[] args = new Class[2];
		  args[0] = ZNRecord.class;
		  args[1] = String.class;
		  Method m = risg.getClass().getDeclaredMethod("computeFuncISandEVofStorageCluster", args);
		  m.setAccessible(true);
		  Field f = risg.getClass().getDeclaredField("_storageAdapter");
		  f.setAccessible(true);
		  f.set(risg, fakeAdapter);

		  ZNRecord zr2 = risg.computeFuncISandEVofStorageCluster(zev, "db");
		  Assert.assertEquals(zr2.getId(),"db");
		  Assert.assertEquals(zr2.getMapField("ucp_0").get("eat1-app33.stg.linkedin.com_11140"),"MASTER");
		  Assert.assertEquals(zr2.getMapField("ucp_1").get("eat1-app33.stg.linkedin.com_11140"),"OFFLINE");
	  }

	  @Test
	  public void testNonEmptyExternalViewWithMapFieldsOnly()
	      throws Exception
	      {
	    RelayIdealStateGenerator risg = new RelayIdealStateGenerator();

	    String is =
	        "{" +
	            "  \"id\" : \"db\"," +
	            "  \"simpleFields\" : {" +
	            "  \"IDEAL_STATE_MODE\" : \"CUSTOMIZED\"" +
	            "   }," +
	            "  \"mapFields\" : {" +
	            "    \"ucp_0\" : {" +
	            "      \"eat1-app33.stg.linkedin.com_11140\" : \"MASTER\"" +
	            "    }," +
	            "    \"ucp_1\" : {" +
	            "      \"eat1-app33.stg.linkedin.com_11140\" : \"SLAVE\"" +
	            "    }" +
	            "  }," +
	            "  \"listFields\" : {" +
	            "  }" +
	            "}";

	    ZNRecordSerializer zs = new ZNRecordSerializer();
	    ZNRecord zr = (ZNRecord) zs.deserialize(is.getBytes());

	    String ev =
	        "{" +
	            "  \"id\" : \"db\"," +
	            "  \"mapFields\" : {" +
	            "    \"ucp_0\" : {" +
	            "      \"eat1-app33.stg.linkedin.com_11140\" : \"MASTER\"" +
	            "    }" +
	            "  }" +
	            "}";
	    ZNRecord zev = (ZNRecord) zs.deserialize(ev.getBytes());

	    StorageAdapter fakeAdapter = EasyMock.createMock(StorageAdapter.class);
	    org.easymock.EasyMock.expect(fakeAdapter.getIdealState("db")).andReturn(zr).anyTimes();
	    EasyMock.replay(fakeAdapter);

	    Class[] args = new Class[2];
	    args[0] = ZNRecord.class;
	    args[1] = String.class;
	    Method m = risg.getClass().getDeclaredMethod("computeFuncISandEVofStorageCluster", args);
	    m.setAccessible(true);
	    Field f = risg.getClass().getDeclaredField("_storageAdapter");
	    f.setAccessible(true);
	    f.set(risg, fakeAdapter);

	    ZNRecord zr2 = risg.computeFuncISandEVofStorageCluster(zev, "db");
	    Assert.assertEquals(zr2.getId(),"db");
	    Assert.assertEquals(zr2.getMapField("ucp_0").get("eat1-app33.stg.linkedin.com_11140"),"MASTER");
	    Assert.assertEquals(zr2.getMapField("ucp_1").get("eat1-app33.stg.linkedin.com_11140"),"OFFLINE");
	  }

	  @Test
	  void testExternalViewHasAllPartitions()
	  throws Exception
	  {
		  RelayIdealStateGenerator risg = new RelayIdealStateGenerator();

		  String is =
         				  "{" +
						  "  \"id\" : \"db\"," +
						  "  \"mapFields\" : {" +
						  "    \"ucp_0\" : {" +
						  "      \"eat1-app33.stg.linkedin.com_11140\" : \"MASTER\"" +
						  "    }," +
						  "    \"ucp_1\" : {" +
						  "      \"eat1-app33.stg.linkedin.com_11140\" : \"SLAVE\"" +
						  "    }" +
						  "  }," +
						  "  \"listFields\" : {" +
						  "  \"ucp_0\" : [\"eat1-app33.stg.linkedin.com_11140\"]," +
						  "  \"ucp_1\" : [\"eat1-app33.stg.linkedin.com_11140\"]" +
						  "  }" +
						  "}";

		  ZNRecordSerializer zs = new ZNRecordSerializer();
		  ZNRecord zr = (ZNRecord) zs.deserialize(is.getBytes());

		  String ev =
		          "{" +
				  "  \"id\" : \"db\"," +
				  "  \"mapFields\" : {" +
				  "    \"ucp_0\" : {" +
				  "      \"eat1-app33.stg.linkedin.com_11140\" : \"MASTER\"" +
				  "    }," +
				  "    \"ucp_1\" : {" +
				  "      \"eat1-app33.stg.linkedin.com_11140\" : \"SLAVE\"" +
				  "    }" +
				  "  }" +
				  "}";
		  ZNRecord zev = (ZNRecord) zs.deserialize(ev.getBytes());

          StorageAdapter fakeAdapter = EasyMock.createMock(StorageAdapter.class);
          org.easymock.EasyMock.expect(fakeAdapter.getIdealState("db")).andReturn(zr).anyTimes();
          EasyMock.replay(fakeAdapter);

          Class[] args = new Class[2];
		  args[0] = ZNRecord.class;
		  args[1] = String.class;
		  Method m = risg.getClass().getDeclaredMethod("computeFuncISandEVofStorageCluster", args);
		  m.setAccessible(true);
		  Field f = risg.getClass().getDeclaredField("_storageAdapter");
		  f.setAccessible(true);
		  f.set(risg, fakeAdapter);

		  ZNRecord zr2 = risg.computeFuncISandEVofStorageCluster(zev, "db");
		  Assert.assertEquals(zr2.getId(),"db");
		  Assert.assertEquals(zr2.getMapField("ucp_0").get("eat1-app33.stg.linkedin.com_11140"),"MASTER");
		  Assert.assertEquals(zr2.getMapField("ucp_1").get("eat1-app33.stg.linkedin.com_11140"),"SLAVE");
	  }

	  @Test
	  void testExternalViewMultipleRelays1()
	  throws Exception
	  {
		  RelayIdealStateGenerator risg = new RelayIdealStateGenerator();

		  String is =
         				  "{" +
						  "  \"id\" : \"db\"," +
						  "  \"mapFields\" : {" +
						  "    \"ucp_0\" : {" +
						  "      \"eat1-app33.stg.linkedin.com_11140\" : \"MASTER\"," +
						  "      \"eat1-app34.stg.linkedin.com_11140\" : \"SLAVE\"" +
						  "    }," +
						  "    \"ucp_1\" : {" +
						  "      \"eat1-app33.stg.linkedin.com_11140\" : \"SLAVE\"," +
						  "      \"eat1-app34.stg.linkedin.com_11140\" : \"MASTER\"" +
						  "    }" +
						  "  }," +
						  "  \"listFields\" : {" +
						  "  \"ucp_0\" : [\"eat1-app33.stg.linkedin.com_11140\",\"eat1-app34.stg.linkedin.com_11140\"]," +
						  "  \"ucp_1\" : [\"eat1-app33.stg.linkedin.com_11140\",\"eat1-app34.stg.linkedin.com_11140\"]" +
						  "  }" +
						  "}";

		  ZNRecordSerializer zs = new ZNRecordSerializer();
		  ZNRecord zr = (ZNRecord) zs.deserialize(is.getBytes());

		  String ev =
		          "{" +
				  "  \"id\" : \"db\"," +
				  "  \"mapFields\" : {" +
				  "    \"ucp_0\" : {" +
				  "      \"eat1-app33.stg.linkedin.com_11140\" : \"MASTER\"" +
				  "    }," +
				  "    \"ucp_1\" : {" +
				  "      \"eat1-app33.stg.linkedin.com_11140\" : \"MASTER\"" +
				  "    }" +
				  "  }" +
				  "}";
		  ZNRecord zev = (ZNRecord) zs.deserialize(ev.getBytes());

          StorageAdapter fakeAdapter = EasyMock.createMock(StorageAdapter.class);
          org.easymock.EasyMock.expect(fakeAdapter.getIdealState("db")).andReturn(zr).anyTimes();
          EasyMock.replay(fakeAdapter);

          Class[] args = new Class[2];
		  args[0] = ZNRecord.class;
		  args[1] = String.class;
		  Method m = risg.getClass().getDeclaredMethod("computeFuncISandEVofStorageCluster", args);
		  m.setAccessible(true);
		  Field f = risg.getClass().getDeclaredField("_storageAdapter");
		  f.setAccessible(true);
		  f.set(risg, fakeAdapter);

		  ZNRecord zr2 = risg.computeFuncISandEVofStorageCluster(zev, "db");
		  Assert.assertEquals(zr2.getId(),"db");
		  Assert.assertEquals(zr2.getMapField("ucp_0").get("eat1-app33.stg.linkedin.com_11140"),"MASTER");
		  Assert.assertEquals(zr2.getMapField("ucp_0").get("eat1-app34.stg.linkedin.com_11140"),"OFFLINE");
		  Assert.assertEquals(zr2.getMapField("ucp_1").get("eat1-app33.stg.linkedin.com_11140"),"MASTER");
		  Assert.assertEquals(zr2.getMapField("ucp_1").get("eat1-app34.stg.linkedin.com_11140"),"OFFLINE");
	  }

	  @Test
	  void testExternalViewMultipleRelays2()
	  throws Exception
	  {
		  RelayIdealStateGenerator risg = new RelayIdealStateGenerator();

		  /*
		   * Test case when one of the partitions is not present at all ( not a valid case in espresso but ..)
		   */
		  String is =
         				  "{" +
						  "  \"id\" : \"db\"," +
						  "  \"mapFields\" : {" +
						  "    \"ucp_0\" : {" +
						  "      \"eat1-app33.stg.linkedin.com_11140\" : \"MASTER\"," +
						  "      \"eat1-app34.stg.linkedin.com_11140\" : \"SLAVE\"" +
						  "    }," +
						  "    \"ucp_1\" : {" +
						  "      \"eat1-app33.stg.linkedin.com_11140\" : \"SLAVE\"," +
						  "      \"eat1-app34.stg.linkedin.com_11140\" : \"MASTER\"" +
						  "    }," +
						  "    \"ucp_2\" : {" +
						  "      \"eat1-app33.stg.linkedin.com_11140\" : \"SLAVE\"," +
						  "      \"eat1-app34.stg.linkedin.com_11140\" : \"SLAVE\"" +
						  "    }" +
						  "  }," +
						  "  \"listFields\" : {" +
						  "  \"ucp_0\" : [\"eat1-app33.stg.linkedin.com_11140\",\"eat1-app34.stg.linkedin.com_11140\"]," +
						  "  \"ucp_1\" : [\"eat1-app33.stg.linkedin.com_11140\",\"eat1-app34.stg.linkedin.com_11140\"]," +
						  "  \"ucp_2\" : [\"eat1-app33.stg.linkedin.com_11140\",\"eat1-app34.stg.linkedin.com_11140\"]" +
						  "  }" +
						  "}";

		  ZNRecordSerializer zs = new ZNRecordSerializer();
		  ZNRecord zr = (ZNRecord) zs.deserialize(is.getBytes());

		  String ev =
		          "{" +
				  "  \"id\" : \"db\"," +
				  "  \"mapFields\" : {" +
				  "    \"ucp_0\" : {" +
				  "      \"eat1-app33.stg.linkedin.com_11140\" : \"MASTER\"" +
				  "    }," +
				  "    \"ucp_1\" : {" +
				  "      \"eat1-app33.stg.linkedin.com_11140\" : \"MASTER\"" +
				  "    }" +
				  "  }" +
				  "}";
		  ZNRecord zev = (ZNRecord) zs.deserialize(ev.getBytes());

          StorageAdapter fakeAdapter = EasyMock.createMock(StorageAdapter.class);
          org.easymock.EasyMock.expect(fakeAdapter.getIdealState("db")).andReturn(zr).anyTimes();
          EasyMock.replay(fakeAdapter);

          Class[] args = new Class[2];
		  args[0] = ZNRecord.class;
		  args[1] = String.class;
		  Method m = risg.getClass().getDeclaredMethod("computeFuncISandEVofStorageCluster", args);
		  m.setAccessible(true);
		  Field f = risg.getClass().getDeclaredField("_storageAdapter");
		  f.setAccessible(true);
		  f.set(risg, fakeAdapter);

		  ZNRecord zr2 = risg.computeFuncISandEVofStorageCluster(zev, "db");
		  Assert.assertEquals(zr2.getId(),"db");
		  Assert.assertEquals(zr2.getMapField("ucp_0").get("eat1-app33.stg.linkedin.com_11140"),"MASTER");
		  Assert.assertEquals(zr2.getMapField("ucp_0").get("eat1-app34.stg.linkedin.com_11140"),"OFFLINE");
		  Assert.assertEquals(zr2.getMapField("ucp_1").get("eat1-app33.stg.linkedin.com_11140"),"MASTER");
		  Assert.assertEquals(zr2.getMapField("ucp_1").get("eat1-app34.stg.linkedin.com_11140"),"OFFLINE");
		  Assert.assertEquals(zr2.getMapField("ucp_2").get("eat1-app33.stg.linkedin.com_11140"),"OFFLINE");
		  Assert.assertEquals(zr2.getMapField("ucp_2").get("eat1-app34.stg.linkedin.com_11140"),"OFFLINE");
	  }

	  @Test
	  public void testDbName()
	  throws Exception
	  {
		  String phyPartition = "test_boris_phani_2";
		  String[] dbName = RelayIdealStateGenerator.parsePhyPartition(phyPartition);
		  Assert.assertEquals("test_boris_phani", dbName[0]);
		  Assert.assertEquals("2", dbName[1]);

		  phyPartition = "test_boris_phani_23";
		  dbName = RelayIdealStateGenerator.parsePhyPartition(phyPartition);
	      Assert.assertEquals("test_boris_phani", dbName[0]);
		  Assert.assertEquals("23", dbName[1]);

	  }

	  @Test
	  public void testDynamicDbAddition()
	  throws Exception
	  {
		  RelayIdealStateGenerator risg = new RelayIdealStateGenerator();

		  String is2 =
		          "{" +
				  "  \"id\" : \"db2\"," +
		          "  \"simpleFields\" : {" +
				  "  \"IDEAL_STATE_MODE\" : \"AUTO\"" +
				  "   }," +
				  "  \"mapFields\" : {" +
				  "    \"ucp_0\" : {" +
				  "      \"eat1-app33.stg.linkedin.com_11140\" : \"MASTER\"" +
				  "    }," +
				  "    \"ucp_1\" : {" +
				  "      \"eat1-app33.stg.linkedin.com_11140\" : \"SLAVE\"" +
				  "    }" +
				  "  }," +
				  "  \"listFields\" : {" +
				  "  \"ucp_0\" : [\"eat1-app33.stg.linkedin.com_11140\"]," +
				  "  \"ucp_1\" : [\"eat1-app33.stg.linkedin.com_11140\"]" +
		     	  "  }" +
         		  "}";

		  ZNRecordSerializer zs = new ZNRecordSerializer();
		  ZNRecord zr2 = (ZNRecord) zs.deserialize(is2.getBytes());

		  String ev2 =
		          "{" +
				  "  \"id\" : \"db2\"," +
				  "  \"mapFields\" : {" +
				  "    \"ucp_0\" : {" +
				  "      \"eat1-app33.stg.linkedin.com_11140\" : \"MASTER\"" +
				  "    }" +
				  "  }" +
				  "}";

		  ZNRecord zev2 = (ZNRecord) zs.deserialize(ev2.getBytes());

          StorageAdapter fakeAdapter = EasyMock.createMock(StorageAdapter.class);
          org.easymock.EasyMock.expect(fakeAdapter.getIdealState("db2")).andReturn(zr2).anyTimes();
          EasyMock.replay(fakeAdapter);

          Class[] args = new Class[2];
		  args[0] = ZNRecord.class;
		  args[1] = String.class;
		  Method m = risg.getClass().getDeclaredMethod("computeFuncISandEVofStorageCluster", args);
		  m.setAccessible(true);
		  Field f = risg.getClass().getDeclaredField("_storageAdapter");
		  f.setAccessible(true);
		  f.set(risg, fakeAdapter);

		  ExternalView evZev2 = new ExternalView(zev2);
		  Set<String> dbNames = new HashSet<String>();
		  dbNames.add("db2");
		  risg.handleDatabaseAdditionRemoval(dbNames);

		  Assert.assertEquals(risg.getDbNames().size(), 1);
		  Assert.assertEquals(risg.getDbNames().contains("db2"), true);
	  }

	  @Test
	  public void testgetIdealStateForDatabase()
	  throws Exception
	  {
		  IdealState fakeIs = EasyMock.createMock(IdealState.class);
		  org.easymock.EasyMock.expect(fakeIs.getStateModelDefRef()).andReturn("SCHEMA_SM");
		  org.easymock.EasyMock.expect(fakeIs.getStateModelDefRef()).andReturn("MasterSlave");
		  EasyMock.replay(fakeIs);
		  StorageAdapter sa = new StorageAdapter("testCluster", "testZk");
		  Assert.assertEquals(sa.isAValidResourceIdealState(fakeIs), false);
		  Assert.assertEquals(sa.isAValidResourceIdealState(fakeIs), true);
	  }
	  
	  @Test
	  public void testDbNameAddition1()
	  throws Exception
	  {
		  RelayIdealStateGenerator risg = new RelayIdealStateGenerator();
		  Assert.assertEquals(risg.getDbNames().size(), 0);
		  Set<String> dbNames = new HashSet<String>();
		  dbNames.add("db1");
		  risg.handleDatabaseAdditionRemoval(dbNames);
		  Assert.assertEquals(risg.getDbNames().size(), 1);
		  
		  // Add the same database ( no-op ). There should be no change
		  dbNames.add("db1"); 
		  risg.handleDatabaseAdditionRemoval(dbNames);
		  Assert.assertEquals(risg.getDbNames().size(), 1);

		  // Add two databases, comprising of the previous one.
		  // Should not trigger a drop database call
		  dbNames.add("db1");
		  dbNames.add("db2");
		  risg.handleDatabaseAdditionRemoval(dbNames);
		  Assert.assertEquals(risg.getDbNames().size(), 2);		  
	}
	  
	  @Test
	  public void testDbNameRemoval1()
	  throws Exception
	  {
		  RelayIdealStateGenerator risg = new RelayIdealStateGenerator();
		  RelaySpectator fakeRelaySpectator = EasyMock.createMock(RelaySpectator.class);
		  fakeRelaySpectator.removeResourceGroup((String)EasyMock.anyObject());
		  EasyMock.expectLastCall().anyTimes();
		  EasyMock.replay(fakeRelaySpectator);

		  ResourceManager rm = EasyMock.createMock(ResourceManager.class);
		  EasyMock.expect(rm.dropDatabase((String)EasyMock.anyObject())).andReturn(true).anyTimes();
		  EasyMock.replay(rm);

		  Field f = risg.getClass().getDeclaredField("_relaySpectator");
		  f.setAccessible(true);
		  f.set(risg, fakeRelaySpectator);

		  f = risg.getClass().getDeclaredField("_rm");
		  f.setAccessible(true);
		  f.set(risg, rm);

		  
		  Assert.assertEquals(risg.getDbNames().size(), 0);
		  Set<String> dbNames = new HashSet<String>();
		  dbNames.add("db1");
		  risg.handleDatabaseAdditionRemoval(dbNames);
		  Assert.assertEquals(risg.getDbNames().size(), 1);
		  
		  // Drop the same database
		  dbNames.clear(); 
		  risg.handleDatabaseAdditionRemoval(dbNames);
		  Assert.assertEquals(risg.getDbNames().size(), 0);

		  // Add two databases, comprising of the previous one.
		  // Should not trigger a drop database call
		  dbNames.add("db1");
		  dbNames.add("db2");
		  risg.handleDatabaseAdditionRemoval(dbNames);
		  Assert.assertEquals(risg.getDbNames().size(), 2);
		  dbNames.remove("db1");
		  risg.handleDatabaseAdditionRemoval(dbNames);
		  Assert.assertEquals(risg.getDbNames().size(), 1);
	  }

}


