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


import java.util.Arrays;
import java.util.Vector;
import java.util.List;
import java.lang.reflect.Field;
import java.text.ParseException;


import org.testng.annotations.Test;
import org.testng.Assert;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.easymock.classextension.EasyMock;

import com.linkedin.databus.core.cmclient.ResourceKey;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus3.espresso.schema.EspressoBackedSchemaRegistryService;

public class TestClusterManagerPhysicalSourceConfigBuilder
{

	public static final Logger LOG = Logger.getLogger(TestClusterManagerPhysicalSourceConfigBuilder.class);

	static
	{
		PatternLayout defaultLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c} %m%n");
		ConsoleAppender defaultAppender = new ConsoleAppender(defaultLayout);

		Logger.getRootLogger().removeAllAppenders();
		Logger.getRootLogger().addAppender(defaultAppender);

		Logger.getRootLogger().setLevel(Level.OFF);
		Logger.getRootLogger().setLevel(Level.ERROR);
	}

	@Test
	public void testResourceKeyProcessing() 
	throws ParseException, InvalidConfigException {
		Vector<ResourceKey> rks = new Vector<ResourceKey>();
		ResourceKey r1 = new ResourceKey("ela4-db1-espresso.prod.linkedin.com_1521,BizFollow,p1_1,MASTER");
		rks.add(r1);

		MockPhysicalSourceConfigBuilder mpscb = new MockPhysicalSourceConfigBuilder(rks);
		List<PhysicalSourceStaticConfig> pssc = mpscb.build();
		
		Assert.assertEquals(pssc.size(), 1);
		Assert.assertEquals(pssc.get(0).getName(), "BizFollow");
		Assert.assertEquals(pssc.get(0).getUri(),  "ela4-db1-espresso.prod.linkedin.com_1521");
		Assert.assertEquals(pssc.get(0).getRole(), "MASTER");
		Assert.assertEquals(pssc.get(0).getSources()[0].getName() , "BizFollow.BizFollowData");
	}
	
	@Test
	public void testDifferentPartitions() 
	throws ParseException, InvalidConfigException {
		Vector<ResourceKey> rks = new Vector<ResourceKey>();
		ResourceKey r1 = new ResourceKey("ela4-db1-espresso.prod.linkedin.com_1521,BizFollow,p1_1,MASTER");
		ResourceKey r2 = new ResourceKey("ela4-db1-espresso.prod.linkedin.com_1521,BizFollow,p2_1,MASTER");
		rks.add(r1);
		rks.add(r2);
		
		MockPhysicalSourceConfigBuilder mpscb = new MockPhysicalSourceConfigBuilder(rks);
		List<PhysicalSourceStaticConfig> pssc = mpscb.build();
		
		Assert.assertEquals(pssc.size(), 2);

		Assert.assertEquals(pssc.get(0).getName(), "BizFollow");
		Assert.assertEquals(pssc.get(0).getUri(),  "ela4-db1-espresso.prod.linkedin.com_1521");
		Assert.assertEquals(pssc.get(0).getRole(),  "MASTER");
		Assert.assertEquals(pssc.get(0).getSources()[0].getName() , "BizFollow.BizFollowData");
		
		Assert.assertEquals(pssc.get(1).getName(), "BizFollow");
		Assert.assertEquals(pssc.get(1).getUri(),  "ela4-db1-espresso.prod.linkedin.com_1521");
		Assert.assertEquals(pssc.get(1).getRole(),  "MASTER");
		Assert.assertEquals(pssc.get(1).getSources()[0].getName() , "BizFollow.BizFollowData");
	}

	@Test
	public void testLogicalSourcesCaching() 
	throws ParseException, InvalidConfigException, DatabusException, NoSuchFieldException, IllegalAccessException {
		Vector<ResourceKey> rks = new Vector<ResourceKey>();
		ResourceKey r1 = new ResourceKey("ela4-db1-espresso.prod.linkedin.com_1521,BizFollow,p1_1,MASTER");
		rks.add(r1);

		MockPhysicalSourceConfigBuilder mpscb = new MockPhysicalSourceConfigBuilder(rks);
		
		EspressoBackedSchemaRegistryService fakeRegistry = EasyMock.createMock(EspressoBackedSchemaRegistryService.class);
		List<String> expectedTables = Arrays.asList("BizCSAdmins");
		fakeRegistry.loadAllSourcesOnlyIfNewEspressoDB("BizFollow");
		org.easymock.EasyMock.expectLastCall().once();
		EasyMock.expect(fakeRegistry.getAllLogicalSourcesInDatabase("BizFollow")).andReturn(expectedTables).anyTimes();
		EasyMock.replay(fakeRegistry);

		mpscb._schemaRegistryService = fakeRegistry;
		
		try 
		{
			mpscb.getLogicalSourcesFromRegistry("BizFollowCachingTest");			
		} catch (Exception e){}
		Assert.assertEquals(mpscb._cacheHitCount.get("BizFollow").intValue(), 0);
		
		try
		{
			mpscb.getLogicalSourcesFromRegistry("BizFollowCachingTest");			
		} catch (Exception e){}
		Assert.assertEquals(mpscb._cacheHitCount.get("BizFollow").intValue(), 1);

	}
	

}
