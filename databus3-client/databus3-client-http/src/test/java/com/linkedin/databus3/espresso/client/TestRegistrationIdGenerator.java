package com.linkedin.databus3.espresso.client;
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


import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.client.pub.RegistrationId;
import com.linkedin.databus.client.registration.RegistrationIdGenerator;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus2.test.TestUtil;

public class TestRegistrationIdGenerator {

    @BeforeClass
    public void setupClass()
    {
      TestUtil.setupLogging(true, null, Level.ERROR);
    }

    @Test(groups={"unit", "fast"})
	public void testGeneration1()
	throws Exception
	{
		List<DatabusSubscription> ds = new ArrayList<DatabusSubscription>();
		ds.add(DatabusSubscription.createMasterSourceSubscription(LogicalSource.createAllSourcesWildcard()));

		String id1 = RegistrationIdGenerator.generateNewId(new String(), ds).getId();
		String id2 = RegistrationIdGenerator.generateNewId(new String(), ds).getId();
		AssertJUnit.assertFalse(id1.equals(id2));

		// The second RegistrationId for a consumer with the same subscription will be the same id, with a suffix
		// for the count at the end.
		AssertJUnit.assertTrue(id2.startsWith(id1));
		return;
	}

    @Test(groups={"unit", "fast"})
	public void testInvalidity()
	throws Exception
	{
		RegistrationId rid = new RegistrationId("test123");
		RegistrationIdGenerator.insertId(rid);
		AssertJUnit.assertFalse(RegistrationIdGenerator.isIdValid(rid));
		return;
	}

}
