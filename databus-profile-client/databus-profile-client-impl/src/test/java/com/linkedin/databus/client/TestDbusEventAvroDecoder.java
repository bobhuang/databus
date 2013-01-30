package com.linkedin.databus.client;
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


import java.nio.ByteBuffer;
import java.nio.channels.Channels;

import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.KeyTypeNotImplementedException;
import com.linkedin.databus.relay.member2.DatabusEventProfileRandomProducer;
import com.linkedin.databus2.schemas.VersionedSchema;
import com.linkedin.databus2.schemas.VersionedSchemaSet;
import com.linkedin.databus2.schemas.utils.Utils;
import com.linkedin.events.member2.profile.MemberProfile_V3;

public class TestDbusEventAvroDecoder
{
  public static final Logger LOG = Logger.getLogger(TestDbusEventAvroDecoder.class.getName());

  static
  {
    PatternLayout defaultLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c{1}} %m%n");
    ConsoleAppender defaultAppender = new ConsoleAppender(defaultLayout);

    Logger.getRootLogger().removeAllAppenders();
    Logger.getRootLogger().addAppender(defaultAppender);

    Logger.getRootLogger().setLevel(Level.OFF);
    //Logger.getRootLogger().setLevel(Level.ERROR);
    //Logger.getRootLogger().setLevel(Level.WARN);
    //Logger.getRootLogger().setLevel(Level.INFO);
  }

	private long executeTest(boolean performantMode) throws KeyTypeNotImplementedException
	{
		VersionedSchemaSet schemaSet = new VersionedSchemaSet();
		schemaSet.add(new VersionedSchema("com.linked.events.member2.profile.MemberProfile", (short)3,
		                                  MemberProfile_V3.SCHEMA$));
		MemberProfile_V3 reuse = new MemberProfile_V3();
		int minLength = 100;
		int maxLength = 200;
		byte[] schemaId = Utils.md5(MemberProfile_V3.SCHEMA$.toString().getBytes());
		ByteBuffer serializationBuffer = ByteBuffer.allocate(1000000).order(DbusEvent.byteOrder);
		for (int i = 0; i < 10; ++i)
		{
			GenericRecord r = DatabusEventProfileRandomProducer.produceOneProfileEvent(MemberProfile_V3.SCHEMA$, minLength, maxLength);
			byte[] value = DatabusEventProfileRandomProducer.serializeEvent(MemberProfile_V3.SCHEMA$, r);
			DbusEvent.serializeEvent(new DbusEventKey(1234), (short)0, (short)30, System.nanoTime(), (short)2, schemaId , value, false, serializationBuffer);
		}

		DbusEventAvroDecoder decoder = new DbusEventAvroDecoder(schemaSet);
		int numIterations = 50000;
		long startTime = System.nanoTime();
		for (int j = 0; j < numIterations; ++j)
		{
			int position = 0;
			for (int i = 0; i < 10; ++i)
			{
				DbusEvent e = new DbusEvent(serializationBuffer, position);
				reuse = decoder.getTypedValue(e, reuse, MemberProfile_V3.class);
				//LOG.info(reuse);
				position += e.size();
			}
		}
		long endTime = System.nanoTime();
		return (endTime - startTime);
	}

	@Test
	public void testGetTypedValue() throws KeyTypeNotImplementedException
	{
		long fastTime = executeTest(true);
		long slowTime = executeTest(false);
		//Assert.assertFalse(slowTime  <= fastTime);
		LOG.info("Slow Version: Time in millis = " + (slowTime)/1000000.0);
		LOG.info("Fast Version: Time in millis = " + (fastTime)/1000000.0);
	}

	@Test
	public void testDumpEventValueInJSON() throws KeyTypeNotImplementedException
	{
		//TODO: Add some real tests here
		VersionedSchemaSet schemaSet = new VersionedSchemaSet();
		schemaSet.add(new VersionedSchema("com.linked.events.member2.profile.MemberProfile", (short)1,
		                                  MemberProfile_V3.SCHEMA$));
		int minLength = 100;
		int maxLength = 200;
		byte[] schemaId = Utils.md5(MemberProfile_V3.SCHEMA$.toString().getBytes());
		ByteBuffer serializationBuffer = ByteBuffer.allocate(1000000).order(DbusEvent.byteOrder);
		for (int i = 0; i < 10; ++i)
		{
			GenericRecord r = DatabusEventProfileRandomProducer.produceOneProfileEvent(MemberProfile_V3.SCHEMA$, minLength, maxLength);
			byte[] value = DatabusEventProfileRandomProducer.serializeEvent(MemberProfile_V3.SCHEMA$, r);
			DbusEvent.serializeEvent(new DbusEventKey(1234), (short)0, (short)30, System.nanoTime(), (short)2, schemaId , value, false, serializationBuffer);
		}

		DbusEventAvroDecoder decoder = new DbusEventAvroDecoder(schemaSet);
		int numIterations = 2;
		long startTime = System.nanoTime();
		for (int j = 0; j < numIterations; ++j)
		{
			int position = 0;
			for (int i = 0; i < 10; ++i)
			{
				DbusEvent e = new DbusEvent(serializationBuffer, position);
				//decoder.dumpEventValueInJSON(e, Channels.newChannel(System.out));
				position += e.size();
			}
		}
		long endTime = System.nanoTime();
		LOG.info("Total time in millis = " + (endTime - startTime)/1000000.0);
	}
}
