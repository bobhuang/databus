package com.linkedin.databus2.hadoop.seeding.anetmembers;

import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ToolRunner;

import com.linkedin.databus2.hadoop.seeding.AvroSchemaConverterMain;


public class AnetMembersSchemaConverterMain
  extends AvroSchemaConverterMain
{
	public static final String INPUT_SCHEMA_STR = 
			"{ \"name\" : \"AnetMembers_V7_Intermediate\",  \"doc\" : \"Auto-generated Avro schema for anet.sy$anet_members_6. Generated at Feb 10, 2012 05:41:48 PM PST\",  \"type\" : \"record\",  \"meta\" : \"dbFieldName=anet.sy$anet_members_6;pk=membershipId;\",  \"namespace\" : \"com.linkedin.events.anet\",  \"fields\" : [ {    \"name\" : \"txn\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=TXN;dbFieldPosition=0;\"  }, {    \"name\" : \"membershipId\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=MEMBERSHIP_ID;dbFieldPosition=1;\"  }, {    \"name\" : \"memberId\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=MEMBER_ID;dbFieldPosition=2;\"  }, {    \"name\" : \"anetId\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=ANET_ID;dbFieldPosition=3;\"  }, {    \"name\" : \"entityId\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=ENTITY_ID;dbFieldPosition=4;\"  }, {    \"name\" : \"anetType\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=ANET_TYPE;dbFieldPosition=5;\"  }, {    \"name\" : \"isPrimaryAnet\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=IS_PRIMARY_ANET;dbFieldPosition=6;\"  }, {    \"name\" : \"state\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=STATE;dbFieldPosition=7;\"  }, {    \"name\" : \"contactEmail\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=CONTACT_EMAIL;dbFieldPosition=8;\"  }, {    \"name\" : \"joinedOn\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=JOINED_ON;dbFieldPosition=9;\"  }, {    \"name\" : \"resignedOn\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=RESIGNED_ON;dbFieldPosition=10;\"  }, {    \"name\" : \"lastTransitionOn\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=LAST_TRANSITION_ON;dbFieldPosition=11;\"  }, {    \"name\" : \"createdAt\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=CREATED_AT;dbFieldPosition=12;\"  }, {    \"name\" : \"updatedAt\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=UPDATED_AT;dbFieldPosition=13;\"  }, {    \"name\" : \"locale\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=LOCALE;dbFieldPosition=14;\"  }, {    \"name\" : \"mgmtLevel\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=MGMT_LEVEL;dbFieldPosition=15;\"  }, {    \"name\" : \"mgmtTransitionOn\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=MGMT_TRANSITION_ON;dbFieldPosition=16;\"  }, {    \"name\" : \"settingsEntityId\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=ENTITY_ID;dbFieldPosition=0;\"  }, {    \"name\" : \"settingsSettingId\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=SETTING_ID;dbFieldPosition=1;\"  }, {    \"name\" : \"settingsSettingValue\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=SETTING_VALUE;dbFieldPosition=2;\"  }, {    \"name\" : \"settingsDateCreated\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=DATE_CREATED;dbFieldPosition=3;\"  }, {    \"name\" : \"settingsDateModified\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=DATE_MODIFIED;dbFieldPosition=4;\"  }, {    \"name\" : \"writeAccessLevel\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=WRITE_ACCESS_LEVEL;dbFieldPosition=18;\"  } ]}";
	
	public static final Schema INPUT_SCHEMA = Schema.parse(INPUT_SCHEMA_STR);
		
	public static final String OUTPUT_SCHEMA_STR =
			"{  \"name\" : \"AnetMembers_V7\",  \"doc\" : \"Auto-generated Avro schema for anet.sy$anet_members_6. Generated at Feb 10, 2012 05:41:48 PM PST\",  \"type\" : \"record\",  \"meta\" : \"dbFieldName=anet.sy$anet_members_6;pk=membershipId;\",  \"namespace\" : \"com.linkedin.events.anet\",  \"fields\" : [ {    \"name\" : \"txn\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=TXN;dbFieldPosition=0;\"  }, {    \"name\" : \"membershipId\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=MEMBERSHIP_ID;dbFieldPosition=1;\"  }, {    \"name\" : \"memberId\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=MEMBER_ID;dbFieldPosition=2;\"  }, {    \"name\" : \"anetId\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=ANET_ID;dbFieldPosition=3;\"  }, {    \"name\" : \"entityId\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=ENTITY_ID;dbFieldPosition=4;\"  }, {    \"name\" : \"anetType\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=ANET_TYPE;dbFieldPosition=5;\"  }, {    \"name\" : \"isPrimaryAnet\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=IS_PRIMARY_ANET;dbFieldPosition=6;\"  }, {    \"name\" : \"state\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=STATE;dbFieldPosition=7;\"  }, {    \"name\" : \"contactEmail\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=CONTACT_EMAIL;dbFieldPosition=8;\"  }, {    \"name\" : \"joinedOn\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=JOINED_ON;dbFieldPosition=9;\"  }, {    \"name\" : \"resignedOn\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=RESIGNED_ON;dbFieldPosition=10;\"  }, {    \"name\" : \"lastTransitionOn\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=LAST_TRANSITION_ON;dbFieldPosition=11;\"  }, {    \"name\" : \"createdAt\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=CREATED_AT;dbFieldPosition=12;\"  }, {    \"name\" : \"updatedAt\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=UPDATED_AT;dbFieldPosition=13;\"  }, {    \"name\" : \"locale\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=LOCALE;dbFieldPosition=14;\"  }, {    \"name\" : \"mgmtLevel\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=MGMT_LEVEL;dbFieldPosition=15;\"  }, {    \"name\" : \"mgmtTransitionOn\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=MGMT_TRANSITION_ON;dbFieldPosition=16;\"  }, {    \"name\" : \"settings\",    \"type\" :[{      \"name\" : \"SETTINGS_T\",      \"type\" : \"record\",      \"fields\" : [ {        \"name\" : \"settings\",        \"type\" : {          \"items\" : {            \"name\" : \"settingT\",            \"type\" : \"record\",            \"meta\" : \"dbFieldName=SETTINGS;dbFieldPosition=0;\",            \"fields\" : [ {              \"name\" : \"entityId\",              \"type\" : [ \"long\", \"null\" ],              \"meta\" : \"dbFieldName=ENTITY_ID;dbFieldPosition=0;\"            }, {              \"name\" : \"settingId\",              \"type\" : [ \"long\", \"null\" ],              \"meta\" : \"dbFieldName=SETTING_ID;dbFieldPosition=1;\"            }, {              \"name\" : \"settingValue\",              \"type\" : [ \"string\", \"null\" ],              \"meta\" : \"dbFieldName=SETTING_VALUE;dbFieldPosition=2;\"            }, {              \"name\" : \"dateCreated\",              \"type\" : [ \"long\", \"null\" ],              \"meta\" : \"dbFieldName=DATE_CREATED;dbFieldPosition=3;\"            }, {              \"name\" : \"dateModified\",              \"type\" : [ \"long\", \"null\" ],              \"meta\" : \"dbFieldName=DATE_MODIFIED;dbFieldPosition=4;\"            } ]          },          \"name\" : \"settingsArray\",          \"type\" : \"array\"        }      } ]    },\"null\"],    \"meta\" : \"dbFieldName=SETTINGS;dbFieldPosition=17;\"  }, {    \"name\" : \"writeAccessLevel\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=WRITE_ACCESS_LEVEL;dbFieldPosition=18;\"  } ]}";

	public static final Schema OUTPUT_SCHEMA = Schema.parse(OUTPUT_SCHEMA_STR);

	public static final Schema KEY_FIELD_SCHEMA = Schema.create(Schema.Type.LONG);;
	public static final String KEY_FIELD_NAME = "membershipId";

	
	public static class AnetMembersMapper extends
		SchemaConverterMapper<Long>
	{
		public AnetMembersMapper() 
		{
			super(KEY_FIELD_NAME, new LongKeyPairCreator());
		}		
	}
	
	
	public static class AnetMembersReducer extends
		SchemaConverterReducer<Long>
	{		
		private static final String subRecordSchemaStr = 
				"{ \"name\": \"SETTINGS_T\", \"type\": \"record\", \"fields\": [ { \"name\": \"settings\", \"type\": { \"items\": { \"name\": \"settingT\", \"type\": \"record\", \"meta\": \"dbFieldName=SETTINGS;dbFieldPosition=0;\", \"fields\": [ { \"name\": \"entityId\", \"type\": [ \"long\", \"null\" ], \"meta\": \"dbFieldName=ENTITY_ID;dbFieldPosition=0;\" }, { \"name\": \"settingId\", \"type\": [ \"long\", \"null\" ], \"meta\": \"dbFieldName=SETTING_ID;dbFieldPosition=1;\" }, { \"name\": \"settingValue\", \"type\": [ \"string\", \"null\" ], \"meta\": \"dbFieldName=SETTING_VALUE;dbFieldPosition=2;\" }, { \"name\": \"dateCreated\", \"type\": [ \"long\", \"null\" ], \"meta\": \"dbFieldName=DATE_CREATED;dbFieldPosition=3;\" }, { \"name\": \"dateModified\", \"type\": [ \"long\", \"null\" ], \"meta\": \"dbFieldName=DATE_MODIFIED;dbFieldPosition=4;\" } ] }, \"name\": \"settingsArray\", \"type\": \"array\" } } ] }";
		
		private static final String arraySchemaStr = 					
				"{ \"name\" : \"settingsArray\",  \"type\" : \"array\",  \"items\" : { \"name\" : \"settingT\", \"type\" : \"record\", \"meta\" : \"dbFieldName=SETTINGS;dbFieldPosition=0;\", \"fields\" : [ { \"name\" : \"entityId\", \"type\" : [ \"long\", \"null\" ], \"meta\" : \"dbFieldName=ENTITY_ID;dbFieldPosition=0;\" }, { \"name\" : \"settingId\", \"type\" : [ \"long\", \"null\" ], \"meta\" : \"dbFieldName=SETTING_ID;dbFieldPosition=1;\" }, { \"name\" : \"settingValue\", \"type\" : [ \"string\", \"null\" ], \"meta\" : \"dbFieldName=SETTING_VALUE;dbFieldPosition=2;\"}, { \"name\" : \"dateCreated\", \"type\" : [ \"long\", \"null\" ], \"meta\" : \"dbFieldName=DATE_CREATED;dbFieldPosition=3;\"}, { \"name\" : \"dateModified\", \"type\" : [ \"long\", \"null\" ], \"meta\" : \"dbFieldName=DATE_MODIFIED;dbFieldPosition=4;\"} ]}}";

		private static final String elementSchemaStr = 
			"{ \"name\": \"settingT\", \"type\": \"record\", \"meta\": \"dbFieldName=SETTINGS;dbFieldPosition=0;\", \"fields\": [ { \"name\": \"entityId\", \"type\": [ \"long\", \"null\" ], \"meta\": \"dbFieldName=ENTITY_ID;dbFieldPosition=0;\" }, { \"name\": \"settingId\", \"type\": [ \"long\", \"null\" ], \"meta\": \"dbFieldName=SETTING_ID;dbFieldPosition=1;\" }, { \"name\": \"settingValue\", \"type\": [ \"string\", \"null\" ], \"meta\": \"dbFieldName=SETTING_VALUE;dbFieldPosition=2;\" }, { \"name\": \"dateCreated\", \"type\": [ \"long\", \"null\" ], \"meta\": \"dbFieldName=DATE_CREATED;dbFieldPosition=3;\" }, { \"name\": \"dateModified\", \"type\": [ \"long\", \"null\" ], \"meta\": \"dbFieldName=DATE_MODIFIED;dbFieldPosition=4;\" } ]}";
	
		private static final Schema subRecordSchema = Schema.parse(subRecordSchemaStr);
		private static final Schema arraySchema = Schema.parse(arraySchemaStr);
		private static final Schema elementSchema = Schema.parse(elementSchemaStr);


		public AnetMembersReducer() 
		{
			super(OUTPUT_SCHEMA_STR);
		}

		@Override
		public void convertSchema(Iterable<GenericRecord> inputRecords,
				GenericRecord output, Reporter reporter) 
		{			

			
			String[] outputArrayRecordFields = {
				"entityId",
				"settingId",
				"settingValue",
				"dateCreated",
				"dateModified"						
			};
			
			String[] inputArrayRecordFields = {
					"settingsEntityId",
					"settingsSettingId",
					"settingsSettingValue",
					"settingsDateCreated",
					"settingsDateModified"						
			};
			
			String[] mainFields = { 
					"txn", 
					"membershipId", 
					"memberId",
					"anetId",
					"entityId",
					"anetType",
					"isPrimaryAnet",
					"state",
					"contactEmail",
					"joinedOn",
					"resignedOn",
					"lastTransitionOn",
					"createdAt",
					"updatedAt",
					"locale",
					"mgmtLevel",
					"mgmtTransitionOn",
					"writeAccessLevel",					
			};

			
			Iterator<GenericRecord> itr = inputRecords.iterator();
			
			boolean first = true;
		    
			GenericArray<GenericRecord> avroArray = new GenericData.Array<GenericRecord>(0,arraySchema);

		    while (itr.hasNext())
			{
				GenericRecord input = itr.next();
				
				if (first)
				{
					copyFixedFields(mainFields, mainFields, input, output, "membershipId");
					first = false;
				} 
				
		        GenericRecord elemRecord = new GenericData.Record(elementSchema);
				boolean added = copyFixedFields(inputArrayRecordFields, outputArrayRecordFields, input, elemRecord, "settingsEntityId");	
		        
				if (added)
		        	avroArray.add(elemRecord);
			}
		    
		    GenericRecord outerRecord = new GenericData.Record(subRecordSchema);
		    outerRecord.put("settings", avroArray);
		    output.put("settings", outerRecord);
		}		
	}
	
	public static void main(String[] args) throws Exception
	{					
		int exitCode = ToolRunner.run(new AnetMembersSchemaConverterMain(), args);
		System.exit(exitCode);
	}
	
	public AnetMembersSchemaConverterMain()
	{
		super(INPUT_SCHEMA,
			  KEY_FIELD_SCHEMA,
			  OUTPUT_SCHEMA,
			  AnetMembersMapper.class,
			  AnetMembersReducer.class);
	}
}
