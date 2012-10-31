package com.linkedin.databus2.hadoop.seeding.conns;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import com.linkedin.databus2.hadoop.seeding.DatabusBootstrapDBRowCreatorMapReduce;

public class ConnectionsDBRowCreatorMapReduce extends
		DatabusBootstrapDBRowCreatorMapReduce
{

	/*
	 *  Important Note : The schemaStrArray declaration has to be done very carefully w.r.t
	 *  spaces and newlines and should match the content of ".avsc" file exactly.
	 *  Otherwise schemaId generated will be different from the one client expects.
	 */
	public static final String[] schemaStrArray =  { "{",
		"  \"name\" : \"Connections_V2\",",
		"  \"doc\" : \"Auto-generated Avro schema for conns.sy$connections. Generated at Feb 10, 2012 05:39:40 PM PST\",",
		"  \"type\" : \"record\",",
		"  \"meta\" : \"dbFieldName=conns.sy$connections;\",",
		"  \"namespace\" : \"com.linkedin.events.conns\",",
		"  \"fields\" : [ {",
		"    \"name\" : \"txn\",",
		"    \"type\" : [ \"long\", \"null\" ],",
		"    \"meta\" : \"dbFieldName=TXN;dbFieldPosition=0;\"",
		"  }, {",
		"    \"name\" : \"key\",",
		"    \"type\" : [ \"string\", \"null\" ],",
		"    \"meta\" : \"dbFieldName=KEY;dbFieldPosition=1;\"",
		"  }, {",
		"    \"name\" : \"sourceId\",",
		"    \"type\" : [ \"long\", \"null\" ],",
		"    \"meta\" : \"dbFieldName=SOURCE_ID;dbFieldPosition=2;\"",
		"  }, {",
		"    \"name\" : \"destId\",",
		"    \"type\" : [ \"long\", \"null\" ],",
		"    \"meta\" : \"dbFieldName=DEST_ID;dbFieldPosition=3;\"",
		"  }, {",
		"    \"name\" : \"active\",",
		"    \"type\" : [ \"string\", \"null\" ],",
		"    \"meta\" : \"dbFieldName=ACTIVE;dbFieldPosition=4;\"",
		"  }, {",
		"    \"name\" : \"modifiedDate\",",
		"    \"type\" : [ \"long\", \"null\" ],",
		"    \"meta\" : \"dbFieldName=MODIFIED_DATE;dbFieldPosition=5;\"",
		"  }, {",
		"    \"name\" : \"createDate\",",
		"    \"type\" : [ \"long\", \"null\" ],",
		"    \"meta\" : \"dbFieldName=CREATE_DATE;dbFieldPosition=6;\"",
		"  } ]",
		"}",
		""};

	public static final String schemaStr = arrayToString(schemaStrArray);
	public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse(schemaStr);

	public static final String keyField = "key";
	public static final short srcId = 601;
	//public static final long sequenceId = 211935654834L;

	public static class Map extends AvroBasedMap
	{

		public Map()
		{
			super(SCHEMA$, srcId, keyField);
		}

	}
	@Override
	public int run(String[] args)
		throws Exception
	{
		return super.run(args, "ConnectionsDB MySQL Row Creator", SCHEMA$, 250, Map.class);
	}

	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new Configuration(), new ConnectionsDBRowCreatorMapReduce(), args);
		System.exit(exitCode);
	}
}
