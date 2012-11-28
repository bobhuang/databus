package com.linkedin.databus3.espresso;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.linkedin.databus2.schemas.SchemaId;

public class EmailSchema implements IEspressoSchema {

	private String SOURCE_SCHEMA_STR =
			"{" +
					"   \"name\" : \"Email\"," +
					"   \"version\": 1," +
					"   \"doc\" : \"Espresso Schema for a simple Email Table\"," +
					"   \"type\" : \"record\"," +
					"   \"schemaType\" : \"DocumentSchema\"," +
					"   \"namespace\" : \"com.linkedin.espresso.test.espressodb\"," +
					"   \"fields\" : [ " +
					"   {" +
					"     \"name\" : \"createdDate\"," +
					"     \"type\" : \"long\"," +
					"     \"default\" : 0" +
					"   }, {" +
					"     \"name\" : \"fromMemberId\"," +
					"     \"type\" : \"long\"" +
					"   }, {" +
					"     \"name\" : \"subject\"," +
					"     \"type\" : \"string\"," +
					"     \"indexType\" : \"text\"" +
					"   }, {" +
					"     \"name\" : \"body\"," +
					"     \"type\" : \"string\"," +
					"     \"indexType\" : \"attribute\"" +
					"   } ]" +
					" }";
	
	private short srcId = 16;

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
		r.put("createdDate", 1L);
		r.put("fromMemberId", 1L);
		r.put("subject", new org.apache.avro.util.Utf8("EspressoDatabusIntegrationTest : " + System.currentTimeMillis()));
		r.put("body", new org.apache.avro.util.Utf8(
				"Integration Test with Email Schema"));
		return r;
	}

}
