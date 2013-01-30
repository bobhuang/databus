package com.linkedin.databus.container.request;
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


import java.text.ParseException;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.core.cmclient.ResourceKey;

public class TestClusterManagerConfiguration
{
	static
	{
	  PatternLayout defaultLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c} %m%n");
	  ConsoleAppender defaultAppender = new ConsoleAppender(defaultLayout);

	  Logger.getRootLogger().removeAllAppenders();
	  Logger.getRootLogger().addAppender(defaultAppender);

	  Logger.getRootLogger().setLevel(Level.OFF);
	}

	@BeforeClass
	void setUp()
	{
	}

	// The arguments to be provided to the string are as follows
	// arg1 - PhysicalPartition + "/" + LogicalSource
	// arg2 - PhysicalSource
	// arg3 - LogicalPartition
	static final String configSource = "{\n" +
	"    \"name\" : \"EspressoTest\",\n" +
	"    \"id\" : 100,\n" +
	"    \"uri\" : \"uri1\",\n" +
	"        \"slowSourceQueryThreshold\" : 2000,\n" +
	"        \"sources\" :\n" +
	"        [\n" +
	"                {\"id\" : 1, \n" +
	"                 \"name\" : \"%s\",\n" +
	"                 \"uri\": \"%s\", \n" +
	"                 \"partitionFunction\" : \"constant:1\", \n" +
	"                 \"partition\" : %d \n" +
	"                },\n" +
	"        ]\n" +
	"}";

	@Test
	public void testResourceKey() throws Exception
	{
		// As obtained from cluster manager
		ResourceKey rk = new ResourceKey("ela4-db1-espresso.prod.linkedin.com_1521,bizProfile,p1_1,MASTER");

		Assert.assertEquals(rk.getPhysicalSource(), "ela4-db1-espresso.prod.linkedin.com_1521");
		Assert.assertEquals(rk.getPhysicalPartition(), "bizProfile");
		Assert.assertEquals(rk.getLogicalPartition(), "p1_1");
		Assert.assertEquals(rk.isMaster(), true);
	}

	@Test(expectedExceptions = ParseException.class)
	public void testInvalidResourceKey() throws Exception
	{
		/***
		 * Missing MASTER / SLAVE node
		 */
		new ResourceKey("ela4-db1-espresso.prod.linkedin.com_1521,bizProfile,p1_1");
	}

	@Test(expectedExceptions = ParseException.class)
	public void testInvalidLogicalPartition() throws Exception
	{
		/***
		 * Incorrect logical partition format
		 */
		new ResourceKey("ela4-db1-espresso.prod.linkedin.com_1521,bizProfile,p1,MASTER");
	}

	@Test
	public void testSourceFormatting() throws Exception
	{
		String finalStr = String.format(configSource, "test", "not", 1);
		final String expConfigSource = "{\n" +
		"    \"name\" : \"EspressoTest\",\n" +
		"    \"id\" : 100,\n" +
		"    \"uri\" : \"uri1\",\n" +
		"        \"slowSourceQueryThreshold\" : 2000,\n" +
		"        \"sources\" :\n" +
		"        [\n" +
		"                {\"id\" : 1, \n" +
		"                 \"name\" : \"test\",\n" +
		"                 \"uri\": \"not\", \n" +
		"                 \"partitionFunction\" : \"constant:1\", \n" +
		"                 \"partition\" : 1 \n" +
		"                },\n" +
		"        ]\n" +
		"}";

		Assert.assertEquals(finalStr, expConfigSource);
		return;
	}
}
