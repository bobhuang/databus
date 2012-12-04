package com.linkedin.databus3.espresso;

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
