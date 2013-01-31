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


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.linkedin.databus2.schemas.SchemaId;

public class IdNamePairSchema implements IEspressoSchema {
	
	private String SOURCE_SCHEMA_STR =
			"{" +
					"   \"name\" : \"IdNamePair\"," +
					"   \"version\": 1," +
					"   \"doc\" : \"Espresso Schema for a simple Id Name Pair Table\"," +
					"   \"type\" : \"record\"," +
					"   \"schemaType\" : \"DocumentSchema\"," +
					"   \"namespace\" : \"com.linkedin.espresso.test.espressodb\"," +
					"   \"fields\" : [ " +
					"   {" +
					"     \"name\" : \"id\"," +
					"     \"type\" : \"int\"," +
					"   }, {" +
					"     \"name\" : \"name\"," +
					"     \"type\" : \"string\"" +
					"   } ]" +
					" }";
	
	private short srcId = 19;

	@Override
	public Schema getSchema()
	{
		return Schema.parse(SOURCE_SCHEMA_STR);
	}
	
	@Override
	public SchemaId getSchemaId()
	{
		Schema s = getSchema();
		return SchemaId.forSchema(s);
	}
	
	@Override
	public short getSrcId()
	{
		return srcId;
	}
	
	@Override
	public GenericRecord createDataPerSchema()
	{
		Schema s = getSchema();
		GenericRecord r = new GenericData.Record(s);
		r.put("id", 1L);
		r.put("name",  new org.apache.avro.util.Utf8("EspressoDatabusIntegrationTest : " + System.currentTimeMillis()));
		return r;
	}
}
