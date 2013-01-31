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


import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import com.linkedin.databus.client.pub.ServerV3Info;
import com.linkedin.databus.client.pub.DatabusServerCoordinates.StateId;
import com.linkedin.databus.core.cmclient.ResourceKey;

public class TestServerV3Info {

	@Test
	public void testServerInfoCreation()
	throws Exception
	{
		String name = "testEspressoRelay";
		int port = 10001;
		InetSocketAddress addr = new InetSocketAddress(InetAddress.getLocalHost(), port);
		new ServerV3Info( name, StateId.ONLINE.name(), addr);
	}
	
	@Test
	public void testServerInfoResourceKeyAdd()
	throws Exception
	{
		String name = "testEspressoRelay";
		int port = 10001;
		InetSocketAddress addr = new InetSocketAddress(InetAddress.getLocalHost(), port);
		ServerV3Info svi = new ServerV3Info(name, StateId.ONLINE.name(), addr);
		
		ResourceKey rk = new ResourceKey("ela4-db1-espresso.prod.linkedin.com_1521,bizProfile,p1_1,MASTER");
		svi.addResourceKey(rk);
		
		AssertJUnit.assertSame(svi.getResourceKeys().get(0), rk);
	}

}
