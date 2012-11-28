package com.linkedin.databus2.hadoop.seeding.anets;

import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ToolRunner;

import com.linkedin.databus2.hadoop.seeding.AvroSchemaConverterMain;


public class AnetsSchemaConverterMain extends AvroSchemaConverterMain
{
	public static final String INPUT_SCHEMA_STR = 
			"{ \"name\" : \"Anets_V6_Intermediate\",  \"doc\" : \"Auto-generated Avro schema for anet.sy$anets_5. Generated at Feb 10, 2012 05:41:10 PM PST\",  \"type\" : \"record\",  \"meta\" : \"dbFieldName=anet.sy$anets_5;\",  \"namespace\" : \"com.linkedin.events.anet\",  \"fields\" : [ {    \"name\" : \"txn\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=TXN;dbFieldPosition=0;\"  }, {    \"name\" : \"key\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=KEY;dbFieldPosition=1;\"  }, {    \"name\" : \"anetId\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=ANET_ID;dbFieldPosition=2;\"  }, {    \"name\" : \"anetType\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=ANET_TYPE;dbFieldPosition=3;\"  }, {    \"name\" : \"entityId\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=ENTITY_ID;dbFieldPosition=4;\"  }, {    \"name\" : \"state\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=STATE;dbFieldPosition=5;\"  }, {    \"name\" : \"name\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=NAME;dbFieldPosition=6;\"  }, {    \"name\" : \"locale\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=LOCALE;dbFieldPosition=7;\"  }, {    \"name\" : \"createdAt\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=CREATED_AT;dbFieldPosition=8;\"  }, {    \"name\" : \"updatedAt\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=UPDATED_AT;dbFieldPosition=9;\"  }, {    \"name\" : \"urlKey\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=URL_KEY;dbFieldPosition=10;\"  }, {    \"name\" : \"privateKey\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=PRIVATE_KEY;dbFieldPosition=11;\"  }, {    \"name\" : \"ownerId\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=OWNER_ID;dbFieldPosition=12;\"  }, {    \"name\" : \"shortDescription\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=SHORT_DESCRIPTION;dbFieldPosition=13;\"  }, {    \"name\" : \"description\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=DESCRIPTION;dbFieldPosition=14;\"  }, {    \"name\" : \"largeLogoId\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=LARGE_LOGO_ID;dbFieldPosition=15;\"  }, {    \"name\" : \"smallLogoId\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=SMALL_LOGO_ID;dbFieldPosition=16;\"  }, {    \"name\" : \"vanityUrl\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=VANITY_URL;dbFieldPosition=17;\"  }, {    \"name\" : \"category\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=CATEGORY;dbFieldPosition=18;\"  }, {    \"name\" : \"otherCategory\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=OTHER_CATEGORY;dbFieldPosition=19;\"  }, {    \"name\" : \"contactEmail\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=CONTACT_EMAIL;dbFieldPosition=20;\"  }, {    \"name\" : \"homeSite\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=HOME_SITE;dbFieldPosition=21;\"  }, {    \"name\" : \"country\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=COUNTRY;dbFieldPosition=0;\"  }, {    \"name\" : \"postalCode\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=POSTAL_CODE;dbFieldPosition=1;\"  }, {    \"name\" : \"geoPostalCode\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=GEO_POSTAL_CODE;dbFieldPosition=2;\"  }, {    \"name\" : \"regionCode\",    \"type\" : [ \"int\", \"null\" ],    \"meta\" : \"dbFieldName=REGION_CODE;dbFieldPosition=3;\"  }, {    \"name\" : \"geoPlaceCode\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=GEO_PLACE_CODE;dbFieldPosition=4;\"  }, {    \"name\" : \"geoPlaceMaskCode\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=GEO_PLACE_MASK_CODE;dbFieldPosition=5;\"  }, {    \"name\" : \"latitudeDeg\",    \"type\" : [ \"float\", \"null\" ],    \"meta\" : \"dbFieldName=LATITUDE_DEG;dbFieldPosition=6;\"  }, {    \"name\" : \"longitudeDeg\",    \"type\" : [ \"float\", \"null\" ],    \"meta\" : \"dbFieldName=LONGITUDE_DEG;dbFieldPosition=7;\"  }, {    \"name\" : \"anetSize\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=ANET_SIZE;dbFieldPosition=23;\"  }, {    \"name\" : \"settingsEntityId\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=ENTITY_ID;dbFieldPosition=0;\"  }, {    \"name\" : \"settingsSettingId\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=SETTING_ID;dbFieldPosition=1;\"  }, {    \"name\" : \"settingsSettingValue\",    \"type\" : [ \"string\", \"null\" ],    \"meta\" : \"dbFieldName=SETTING_VALUE;dbFieldPosition=2;\"  }, {    \"name\" : \"settingsDateCreated\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=DATE_CREATED;dbFieldPosition=3;\"  }, {    \"name\" : \"settingsDateModified\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=DATE_MODIFIED;dbFieldPosition=4;\"  }, {    \"name\" : \"parentAnetId\",    \"type\" : [ \"long\", \"null\" ],    \"meta\" : \"dbFieldName=PARENT_ANET_ID;dbFieldPosition=25;\"  } ]}";
		
	public static final Schema INPUT_SCHEMA = Schema.parse(INPUT_SCHEMA_STR);
		
	public static final String OUTPUT_SCHEMA_STR =
			"{ \"name\" : \"Anets_V6\", \"doc\" : \"Auto-generated Avro schema for anet.sy$anets_5. Generated at Feb 10, 2012 05:41:10 PM PST\", \"type\" : \"record\", \"meta\" : \"dbFieldName=anet.sy$anets_5;\", \"namespace\" : \"com.linkedin.events.anet\", \"fields\" : [ { \"name\" : \"txn\", \"type\" : [ \"long\", \"null\" ], \"meta\" : \"dbFieldName=TXN;dbFieldPosition=0;\" }, { \"name\" : \"key\", \"type\" : [ \"long\", \"null\" ], \"meta\" : \"dbFieldName=KEY;dbFieldPosition=1;\" }, { \"name\" : \"anetId\", \"type\" : [ \"long\", \"null\" ], \"meta\" : \"dbFieldName=ANET_ID;dbFieldPosition=2;\" }, { \"name\" : \"anetType\", \"type\" : [ \"string\", \"null\" ], \"meta\" : \"dbFieldName=ANET_TYPE;dbFieldPosition=3;\" }, { \"name\" : \"entityId\", \"type\" : [ \"long\", \"null\" ], \"meta\" : \"dbFieldName=ENTITY_ID;dbFieldPosition=4;\" }, { \"name\" : \"state\", \"type\" : [ \"string\", \"null\" ], \"meta\" : \"dbFieldName=STATE;dbFieldPosition=5;\" }, { \"name\" : \"name\", \"type\" : [ \"string\", \"null\" ], \"meta\" : \"dbFieldName=NAME;dbFieldPosition=6;\" }, { \"name\" : \"locale\", \"type\" : [ \"string\", \"null\" ], \"meta\" : \"dbFieldName=LOCALE;dbFieldPosition=7;\" }, { \"name\" : \"createdAt\", \"type\" : [ \"long\", \"null\" ], \"meta\" : \"dbFieldName=CREATED_AT;dbFieldPosition=8;\" }, { \"name\" : \"updatedAt\", \"type\" : [ \"long\", \"null\" ], \"meta\" : \"dbFieldName=UPDATED_AT;dbFieldPosition=9;\" }, { \"name\" : \"urlKey\", \"type\" : [ \"string\", \"null\" ], \"meta\" : \"dbFieldName=URL_KEY;dbFieldPosition=10;\" }, { \"name\" : \"privateKey\", \"type\" : [ \"string\", \"null\" ], \"meta\" : \"dbFieldName=PRIVATE_KEY;dbFieldPosition=11;\" }, { \"name\" : \"ownerId\", \"type\" : [ \"long\", \"null\" ], \"meta\" : \"dbFieldName=OWNER_ID;dbFieldPosition=12;\" }, { \"name\" : \"shortDescription\", \"type\" : [ \"string\", \"null\" ], \"meta\" : \"dbFieldName=SHORT_DESCRIPTION;dbFieldPosition=13;\" }, { \"name\" : \"description\", \"type\" : [ \"string\", \"null\" ], \"meta\" : \"dbFieldName=DESCRIPTION;dbFieldPosition=14;\" }, { \"name\" : \"largeLogoId\", \"type\" : [ \"string\", \"null\" ], \"meta\" : \"dbFieldName=LARGE_LOGO_ID;dbFieldPosition=15;\" }, { \"name\" : \"smallLogoId\", \"type\" : [ \"string\", \"null\" ], \"meta\" : \"dbFieldName=SMALL_LOGO_ID;dbFieldPosition=16;\" }, { \"name\" : \"vanityUrl\", \"type\" : [ \"string\", \"null\" ], \"meta\" : \"dbFieldName=VANITY_URL;dbFieldPosition=17;\" }, { \"name\" : \"category\", \"type\" : [ \"string\", \"null\" ], \"meta\" : \"dbFieldName=CATEGORY;dbFieldPosition=18;\" }, { \"name\" : \"otherCategory\", \"type\" : [ \"string\", \"null\" ], \"meta\" : \"dbFieldName=OTHER_CATEGORY;dbFieldPosition=19;\" }, { \"name\" : \"contactEmail\", \"type\" : [ \"string\", \"null\" ], \"meta\" : \"dbFieldName=CONTACT_EMAIL;dbFieldPosition=20;\" }, { \"name\" : \"homeSite\", \"type\" : [ \"string\", \"null\" ], \"meta\" : \"dbFieldName=HOME_SITE;dbFieldPosition=21;\" }, { \"name\" : \"geo\", \"type\" : [{ \"name\" : \"DATABUS_GEO_T\", \"type\" : \"record\", \"fields\" : [ { \"name\" : \"country\", \"type\" : [ \"string\", \"null\" ], \"meta\" : \"dbFieldName=COUNTRY;dbFieldPosition=0;\" }, { \"name\" : \"postalCode\", \"type\" : [ \"string\", \"null\" ], \"meta\" : \"dbFieldName=POSTAL_CODE;dbFieldPosition=1;\" }, { \"name\" : \"geoPostalCode\", \"type\" : [ \"string\", \"null\" ], \"meta\" : \"dbFieldName=GEO_POSTAL_CODE;dbFieldPosition=2;\" }, { \"name\" : \"regionCode\", \"type\" : [ \"int\", \"null\" ], \"meta\" : \"dbFieldName=REGION_CODE;dbFieldPosition=3;\" }, { \"name\" : \"geoPlaceCode\", \"type\" : [ \"string\", \"null\" ], \"meta\" : \"dbFieldName=GEO_PLACE_CODE;dbFieldPosition=4;\" }, { \"name\" : \"geoPlaceMaskCode\", \"type\" : [ \"string\", \"null\" ], \"meta\" : \"dbFieldName=GEO_PLACE_MASK_CODE;dbFieldPosition=5;\" }, { \"name\" : \"latitudeDeg\", \"type\" : [ \"float\", \"null\" ], \"meta\" : \"dbFieldName=LATITUDE_DEG;dbFieldPosition=6;\" }, { \"name\" : \"longitudeDeg\", \"type\" : [ \"float\", \"null\" ], \"meta\" : \"dbFieldName=LONGITUDE_DEG;dbFieldPosition=7;\" } ] },\"null\"], \"meta\" : \"dbFieldName=GEO;dbFieldPosition=22;\" }, { \"name\" : \"anetSize\", \"type\" : [ \"long\", \"null\" ], \"meta\" : \"dbFieldName=ANET_SIZE;dbFieldPosition=23;\" }, { \"name\" : \"settings\", \"type\" : [{ \"name\" : \"SETTINGS_T\", \"type\" : \"record\", \"fields\" : [ { \"name\" : \"settings\", \"type\" : { \"items\" : { \"name\" : \"settingT\", \"type\" : \"record\", \"meta\" : \"dbFieldName=SETTINGS;dbFieldPosition=0;\", \"fields\" : [ { \"name\" : \"entityId\", \"type\" : [ \"long\", \"null\" ], \"meta\" : \"dbFieldName=ENTITY_ID;dbFieldPosition=0;\" }, { \"name\" : \"settingId\", \"type\" : [ \"long\", \"null\" ], \"meta\" : \"dbFieldName=SETTING_ID;dbFieldPosition=1;\" }, { \"name\" : \"settingValue\", \"type\" : [ \"string\", \"null\" ], \"meta\" : \"dbFieldName=SETTING_VALUE;dbFieldPosition=2;\" }, { \"name\" : \"dateCreated\", \"type\" : [ \"long\", \"null\" ], \"meta\" : \"dbFieldName=DATE_CREATED;dbFieldPosition=3;\" }, { \"name\" : \"dateModified\", \"type\" : [ \"long\", \"null\" ], \"meta\" : \"dbFieldName=DATE_MODIFIED;dbFieldPosition=4;\" } ] }, \"name\" : \"settingsArray\", \"type\" : \"array\" } } ] },\"null\"], \"meta\" : \"dbFieldName=SETTINGS;dbFieldPosition=24;\" }, { \"name\" : \"parentAnetId\", \"type\" : [ \"long\", \"null\" ], \"meta\" : \"dbFieldName=PARENT_ANET_ID;dbFieldPosition=25;\" } ]}";

	public static final Schema OUTPUT_SCHEMA = Schema.parse(OUTPUT_SCHEMA_STR);

	public static final Schema KEY_FIELD_SCHEMA = Schema.create(Schema.Type.LONG);;
	public static final String KEY_FIELD_NAME = "key";

	
	public static class AnetsMapper extends
		SchemaConverterMapper<Long>
	{
		public AnetsMapper() 
		{
			super(KEY_FIELD_NAME, new LongKeyPairCreator());
		}		
	}
	
	
	public static class AnetsReducer extends
		SchemaConverterReducer<Long>
	{		
		private static final String subRecordSchemaStr = 
				"{ \"name\": \"SETTINGS_T\", \"type\": \"record\", \"fields\": [ { \"name\": \"settings\", \"type\": { \"items\": { \"name\": \"settingT\", \"type\": \"record\", \"meta\": \"dbFieldName=SETTINGS;dbFieldPosition=0;\", \"fields\": [ { \"name\": \"entityId\", \"type\": [ \"long\", \"null\" ], \"meta\": \"dbFieldName=ENTITY_ID;dbFieldPosition=0;\" }, { \"name\": \"settingId\", \"type\": [ \"long\", \"null\" ], \"meta\": \"dbFieldName=SETTING_ID;dbFieldPosition=1;\" }, { \"name\": \"settingValue\", \"type\": [ \"string\", \"null\" ], \"meta\": \"dbFieldName=SETTING_VALUE;dbFieldPosition=2;\" }, { \"name\": \"dateCreated\", \"type\": [ \"long\", \"null\" ], \"meta\": \"dbFieldName=DATE_CREATED;dbFieldPosition=3;\" }, { \"name\": \"dateModified\", \"type\": [ \"long\", \"null\" ], \"meta\": \"dbFieldName=DATE_MODIFIED;dbFieldPosition=4;\" } ] }, \"name\": \"settingsArray\", \"type\": \"array\" } } ] }";
		
		
		private static final String arraySchemaStr = 					
				"{ \"name\" : \"settingsArray\",  \"type\" : \"array\",  \"items\" : { \"name\" : \"settingT\", \"type\" : \"record\", \"meta\" : \"dbFieldName=SETTINGS;dbFieldPosition=0;\", \"fields\" : [ { \"name\" : \"entityId\", \"type\" : [ \"long\", \"null\" ], \"meta\" : \"dbFieldName=ENTITY_ID;dbFieldPosition=0;\" }, { \"name\" : \"settingId\", \"type\" : [ \"long\", \"null\" ], \"meta\" : \"dbFieldName=SETTING_ID;dbFieldPosition=1;\" }, { \"name\" : \"settingValue\", \"type\" : [ \"string\", \"null\" ], \"meta\" : \"dbFieldName=SETTING_VALUE;dbFieldPosition=2;\"}, { \"name\" : \"dateCreated\", \"type\" : [ \"long\", \"null\" ], \"meta\" : \"dbFieldName=DATE_CREATED;dbFieldPosition=3;\"}, { \"name\" : \"dateModified\", \"type\" : [ \"long\", \"null\" ], \"meta\" : \"dbFieldName=DATE_MODIFIED;dbFieldPosition=4;\"} ]}}";

		private static final String elementSchemaStr = 
			"{ \"name\": \"settingT\", \"type\": \"record\", \"meta\": \"dbFieldName=SETTINGS;dbFieldPosition=0;\", \"fields\": [ { \"name\": \"entityId\", \"type\": [ \"long\", \"null\" ], \"meta\": \"dbFieldName=ENTITY_ID;dbFieldPosition=0;\" }, { \"name\": \"settingId\", \"type\": [ \"long\", \"null\" ], \"meta\": \"dbFieldName=SETTING_ID;dbFieldPosition=1;\" }, { \"name\": \"settingValue\", \"type\": [ \"string\", \"null\" ], \"meta\": \"dbFieldName=SETTING_VALUE;dbFieldPosition=2;\" }, { \"name\": \"dateCreated\", \"type\": [ \"long\", \"null\" ], \"meta\": \"dbFieldName=DATE_CREATED;dbFieldPosition=3;\" }, { \"name\": \"dateModified\", \"type\": [ \"long\", \"null\" ], \"meta\": \"dbFieldName=DATE_MODIFIED;dbFieldPosition=4;\" } ]}";
	
		private static final String geoRecordSchemaStr =
			"{ \"name\" : \"DATABUS_GEO_T\",  \"type\" : \"record\", \"fields\" : [ {  \"name\" : \"country\", \"type\" : [ \"string\", \"null\" ],  \"meta\" : \"dbFieldName=COUNTRY;dbFieldPosition=0;\" }, {  \"name\" : \"postalCode\",  \"type\" : [ \"string\", \"null\" ],  \"meta\" : \"dbFieldName=POSTAL_CODE;dbFieldPosition=1;\"  }, {  \"name\" : \"geoPostalCode\",  \"type\" : [ \"string\", \"null\" ],  \"meta\" : \"dbFieldName=GEO_POSTAL_CODE;dbFieldPosition=2;\"  }, { \"name\" : \"regionCode\",  \"type\" : [ \"int\", \"null\" ],  \"meta\" : \"dbFieldName=REGION_CODE;dbFieldPosition=3;\"  }, { \"name\" : \"geoPlaceCode\",  \"type\" : [ \"string\", \"null\" ],  \"meta\" : \"dbFieldName=GEO_PLACE_CODE;dbFieldPosition=4;\"  }, { \"name\" : \"geoPlaceMaskCode\",  \"type\" : [ \"string\", \"null\" ],  \"meta\" : \"dbFieldName=GEO_PLACE_MASK_CODE;dbFieldPosition=5;\"  }, { \"name\" : \"latitudeDeg\",  \"type\" : [ \"float\", \"null\" ],  \"meta\" : \"dbFieldName=LATITUDE_DEG;dbFieldPosition=6;\"  }, { \"name\" : \"longitudeDeg\",  \"type\" : [ \"float\", \"null\" ],  \"meta\" : \"dbFieldName=LONGITUDE_DEG;dbFieldPosition=7;\"  } ]}";

		
		private static final Schema subRecordSchema = Schema.parse(subRecordSchemaStr);
		private static final Schema arraySchema     = Schema.parse(arraySchemaStr);
		private static final Schema elementSchema = Schema.parse(elementSchemaStr);
		private static final Schema geoRecordSchema = Schema.parse(geoRecordSchemaStr);

		
		public AnetsReducer() 
		{
			super(OUTPUT_SCHEMA_STR);
		}

		@Override
		public void convertSchema(Iterable<GenericRecord> inputRecords,
				GenericRecord output, Reporter reporter) 
		{			
			
			String[] geoRecordFields = {
				"country",
				"postalCode",
				"geoPostalCode",
				"regionCode",
				"geoPlaceCode",
				"geoPlaceMaskCode",
				"latitudeDeg",
				"longitudeDeg"
			};
			
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
					"key", 
					"anetId",
					"anetType",
					"entityId",					
					"state",					
					"name",
					"locale",
					"createdAt",
					"updatedAt",
					"urlKey",
					"privateKey",
					"ownerId",
					"shortDescription",
					"description",
					"largeLogoId",
					"smallLogoId",
					"vanityUrl",
					"category",
					"otherCategory",
					"contactEmail",
					"homeSite",
					"anetSize",
					"parentAnetId",
			};

			
			Iterator<GenericRecord> itr = inputRecords.iterator();
			
			boolean first = true;
		    
			GenericArray<GenericRecord> avroArray = new GenericData.Array<GenericRecord>(0,arraySchema);

		    while (itr.hasNext())
			{
				GenericRecord input = itr.next();
								
				if (first)
				{
					copyFixedFields(mainFields, mainFields, input, output, KEY_FIELD_NAME);
					GenericRecord geoRecord = new GenericData.Record(geoRecordSchema);
					copyFixedFields(geoRecordFields, geoRecordFields, input, geoRecord, null);
					output.put("geo", geoRecord);
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
		int exitCode = ToolRunner.run(new AnetsSchemaConverterMain(), args);
		System.exit(exitCode);
	}
	
	public AnetsSchemaConverterMain()
	{
		super(INPUT_SCHEMA,
			  KEY_FIELD_SCHEMA,
			  OUTPUT_SCHEMA,
			  AnetsMapper.class,
			  AnetsReducer.class);
	}
}
