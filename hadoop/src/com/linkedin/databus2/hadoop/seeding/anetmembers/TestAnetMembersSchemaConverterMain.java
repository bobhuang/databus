package com.linkedin.databus2.hadoop.seeding.anetmembers;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

import com.linkedin.events.anet.AnetMembers_V6;
import com.linkedin.events.anet.AnetMembers_V7;
import com.linkedin.events.anet.SETTINGS_T;
import com.linkedin.events.anet.settingT;

public class TestAnetMembersSchemaConverterMain 
{

	@Test
	public void testOneRecordConversion()
		throws Exception
	{
		GenericRecord inputRecord = new GenericData.Record(AnetMembersSchemaConverterMain.INPUT_SCHEMA);
		AnetMembers_V7 anetMember = new AnetMembers_V7();
		
		inputRecord.put("txn", 1L);
		inputRecord.put("membershipId", 2L);
		inputRecord.put("memberId", 3L);
		inputRecord.put("anetId", 4L);
		inputRecord.put("entityId", 5L);
		inputRecord.put("anetType", "A");
		inputRecord.put("isPrimaryAnet", "Y");
		inputRecord.put("state", "CA");
		inputRecord.put("contactEmail", "b@b");
		inputRecord.put("joinedOn", 6L);
		inputRecord.put("resignedOn", 7L);
		inputRecord.put("lastTransitionOn", 8L);
		inputRecord.put("createdAt", 9L);
		inputRecord.put("updatedAt", 10L);
		inputRecord.put("locale", "C");
		inputRecord.put("mgmtLevel", "D");
		inputRecord.put("mgmtTransitionOn", 11L);
		inputRecord.put("settingsEntityId", 12L);
		inputRecord.put("settingsSettingId", 13L);
		inputRecord.put("settingsSettingValue", "E");
		inputRecord.put("settingsDateCreated", 14L);
		inputRecord.put("settingsDateModified", 15L);
		inputRecord.put("writeAccessLevel", "F");
		
		List<GenericRecord> records = new ArrayList<GenericRecord>();
		records.add(inputRecord);
		
		GenericRecord outputRecord = new GenericData.Record(AnetMembersSchemaConverterMain.OUTPUT_SCHEMA);
		AnetMembersSchemaConverterMain.AnetMembersReducer reducer = new AnetMembersSchemaConverterMain.AnetMembersReducer();
		reducer.convertSchema(records, outputRecord, null);
		System.out.println("GenericRecord is : " + outputRecord);

		// Serialize the row
		byte[] serializedValue;
		try
		{
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			Encoder encoder = new BinaryEncoder(bos);
			GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(AnetMembersSchemaConverterMain.OUTPUT_SCHEMA);
			writer.write(outputRecord, encoder);
			serializedValue = bos.toByteArray();
		}
		catch(IOException ex)
		{
			throw new RuntimeException("Failed to serialize the Avro GenericRecord. GenericRecord was :(" + outputRecord + ")", ex);
		}
		
		System.out.println("Bytestream size :" + serializedValue.length);

		BinaryDecoder binaryDecoder = new BinaryDecoder(new ByteArrayInputStream(serializedValue));
				
		JsonDecoder jsonDecoder = new JsonDecoder(AnetMembersSchemaConverterMain.OUTPUT_SCHEMA, outputRecord.toString());

		SpecificDatumReader<AnetMembers_V7> reader = new SpecificDatumReader<AnetMembers_V7>(AnetMembersSchemaConverterMain.OUTPUT_SCHEMA,AnetMembers_V7.SCHEMA$);
		
		reader.read(anetMember,binaryDecoder);
		
		System.out.println("SpecificRecord is : " + anetMember);
		System.out.println("TXN is : " + anetMember.get(0));
		Assert.assertEquals("TXN Check", 1L, anetMember.get(0));
		Assert.assertEquals("membershipId Check", 2L, anetMember.get(1));
		Assert.assertEquals("memberId Check", 3L, anetMember.get(2));
		Assert.assertEquals("anetId Check", 4L, anetMember.get(3));
		Assert.assertEquals("entityId Check", 5L, anetMember.get(4));
		Assert.assertEquals("anetType Check", "A" , anetMember.get(5).toString());
		Assert.assertEquals("isPrimaryAnet Check", "Y", anetMember.get(6).toString());
		Assert.assertEquals("state Check", "CA" , anetMember.get(7).toString());
		Assert.assertEquals("contactEmail Check", "b@b" , anetMember.get(8).toString());
		Assert.assertEquals("joinedOn Check", 6L , anetMember.get(9));
		Assert.assertEquals("resignedOn Check", 7L, anetMember.get(10));
		Assert.assertEquals("lastTransitionOn Check", 8L, anetMember.get(11));
		Assert.assertEquals("createdAt Check", 9L , anetMember.get(12));
		Assert.assertEquals("updatedAt Check", 10L, anetMember.get(13));
		Assert.assertEquals("locale Check", "C" , anetMember.get(14).toString());
		Assert.assertEquals("mgmtLevel Check", "D" , anetMember.get(15).toString());
		Assert.assertEquals("mgmtTransitionOn Check", 11L, anetMember.get(16));
		List<settingT> anetSettings = ((List<settingT>)((SETTINGS_T)(anetMember.get(17))).get(0));
		Assert.assertEquals("settingsEntityId Check", 12L, anetSettings.get(0).get(0));
		Assert.assertEquals("settingsSettingId Check", 13L, anetSettings.get(0).get(1));
		Assert.assertEquals("settingsSettingValue Check", "E", anetSettings.get(0).get(2).toString());
		Assert.assertEquals("settingsDateCreated Check", 14L, anetSettings.get(0).get(3));
		Assert.assertEquals("settingsDateModified Check", 15L, anetSettings.get(0).get(4));
		Assert.assertEquals("writeAccessLevel Check", "F",  anetMember.get(18).toString());
	}
	
	
	@Test
	public void testEmptySettingsRecordConversion()
		throws Exception
	{
		GenericRecord inputRecord = new GenericData.Record(AnetMembersSchemaConverterMain.INPUT_SCHEMA);
		AnetMembers_V7 anetMember = new AnetMembers_V7();
		
		inputRecord.put("txn", 1L);
		inputRecord.put("membershipId", 2L);
		inputRecord.put("memberId", 3L);
		inputRecord.put("anetId", 4L);
		inputRecord.put("entityId", 5L);
		inputRecord.put("anetType", "A");
		inputRecord.put("isPrimaryAnet", "Y");
		inputRecord.put("state", "CA");
		inputRecord.put("contactEmail", "b@b");
		inputRecord.put("joinedOn", 6L);
		inputRecord.put("resignedOn", 7L);
		inputRecord.put("lastTransitionOn", 8L);
		inputRecord.put("createdAt", 9L);
		inputRecord.put("updatedAt", 10L);
		inputRecord.put("locale", "C");
		inputRecord.put("mgmtLevel", "D");
		inputRecord.put("mgmtTransitionOn", 11L);
		inputRecord.put("settingsEntityId", null);
		inputRecord.put("settingsSettingId", null);
		inputRecord.put("settingsSettingValue", "");
		inputRecord.put("settingsDateCreated", null);
		inputRecord.put("settingsDateModified", null);
		inputRecord.put("writeAccessLevel", "F");
		
		List<GenericRecord> records = new ArrayList<GenericRecord>();
		records.add(inputRecord);
		
		GenericRecord outputRecord = new GenericData.Record(AnetMembersSchemaConverterMain.OUTPUT_SCHEMA);
		AnetMembersSchemaConverterMain.AnetMembersReducer reducer = new AnetMembersSchemaConverterMain.AnetMembersReducer();
		reducer.convertSchema(records, outputRecord, null);
		System.out.println("GenericRecord is : " + outputRecord);

		// Serialize the row
		byte[] serializedValue;
		try
		{
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			Encoder encoder = new BinaryEncoder(bos);
			GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(AnetMembersSchemaConverterMain.OUTPUT_SCHEMA);
			writer.write(outputRecord, encoder);
			serializedValue = bos.toByteArray();
		}
		catch(IOException ex)
		{
			throw new RuntimeException("Failed to serialize the Avro GenericRecord. GenericRecord was :(" + outputRecord + ")", ex);
		}
		
		System.out.println("Bytestream size :" + serializedValue.length);

		BinaryDecoder binaryDecoder = new BinaryDecoder(new ByteArrayInputStream(serializedValue));
				
		JsonDecoder jsonDecoder = new JsonDecoder(AnetMembersSchemaConverterMain.OUTPUT_SCHEMA, outputRecord.toString());

		SpecificDatumReader<AnetMembers_V7> reader = new SpecificDatumReader<AnetMembers_V7>(AnetMembersSchemaConverterMain.OUTPUT_SCHEMA,AnetMembers_V7.SCHEMA$);
		
		reader.read(anetMember,binaryDecoder);
		
		System.out.println("SpecificRecord is : " + anetMember);
		System.out.println("TXN is : " + anetMember.get(0));
		Assert.assertEquals("TXN Check", 1L, anetMember.get(0));
		Assert.assertEquals("membershipId Check", 2L, anetMember.get(1));
		Assert.assertEquals("memberId Check", 3L, anetMember.get(2));
		Assert.assertEquals("anetId Check", 4L, anetMember.get(3));
		Assert.assertEquals("entityId Check", 5L, anetMember.get(4));
		Assert.assertEquals("anetType Check", "A" , anetMember.get(5).toString());
		Assert.assertEquals("isPrimaryAnet Check", "Y", anetMember.get(6).toString());
		Assert.assertEquals("state Check", "CA" , anetMember.get(7).toString());
		Assert.assertEquals("contactEmail Check", "b@b" , anetMember.get(8).toString());
		Assert.assertEquals("joinedOn Check", 6L , anetMember.get(9));
		Assert.assertEquals("resignedOn Check", 7L, anetMember.get(10));
		Assert.assertEquals("lastTransitionOn Check", 8L, anetMember.get(11));
		Assert.assertEquals("createdAt Check", 9L , anetMember.get(12));
		Assert.assertEquals("updatedAt Check", 10L, anetMember.get(13));
		Assert.assertEquals("locale Check", "C" , anetMember.get(14).toString());
		Assert.assertEquals("mgmtLevel Check", "D" , anetMember.get(15).toString());
		Assert.assertEquals("mgmtTransitionOn Check", 11L, anetMember.get(16));
		List<settingT> anetSettings = ((List<settingT>)((SETTINGS_T)(anetMember.get(17))).get(0));
		Assert.assertTrue("Settings Empty", anetSettings.isEmpty());
		Assert.assertEquals("writeAccessLevel Check", "F",  anetMember.get(18).toString());
	}
	
	@Test
	public void testMultiRecordsConversion()
		throws Exception
	{
		GenericRecord inputRecord = new GenericData.Record(AnetMembersSchemaConverterMain.INPUT_SCHEMA);
		GenericRecord inputRecord2 = new GenericData.Record(AnetMembersSchemaConverterMain.INPUT_SCHEMA);
		GenericRecord inputRecord3 = new GenericData.Record(AnetMembersSchemaConverterMain.INPUT_SCHEMA);

		AnetMembers_V7 anetMember = new AnetMembers_V7();
		
		inputRecord.put("txn", 1L);
		inputRecord.put("membershipId", 2L);
		inputRecord.put("memberId", 3L);
		inputRecord.put("anetId", 4L);
		inputRecord.put("entityId", 5L);
		inputRecord.put("anetType", "A");
		inputRecord.put("isPrimaryAnet", "Y");
		inputRecord.put("state", "CA");
		inputRecord.put("contactEmail", "b@b");
		inputRecord.put("joinedOn", 6L);
		inputRecord.put("resignedOn", 7L);
		inputRecord.put("lastTransitionOn", 8L);
		inputRecord.put("createdAt", 9L);
		inputRecord.put("updatedAt", 10L);
		inputRecord.put("locale", "C");
		inputRecord.put("mgmtLevel", "D");
		inputRecord.put("mgmtTransitionOn", 11L);
		inputRecord.put("settingsEntityId", 12L);
		inputRecord.put("settingsSettingId", 13L);
		inputRecord.put("settingsSettingValue", "E");
		inputRecord.put("settingsDateCreated", 14L);
		inputRecord.put("settingsDateModified", 15L);
		inputRecord.put("writeAccessLevel", "F");
		
		inputRecord2.put("txn", 1L);
		inputRecord2.put("membershipId", 2L);
		inputRecord2.put("memberId", 3L);
		inputRecord2.put("anetId", 4L);
		inputRecord2.put("entityId", 5L);
		inputRecord2.put("anetType", "A");
		inputRecord2.put("isPrimaryAnet", "Y");
		inputRecord2.put("state", "CA");
		inputRecord2.put("contactEmail", "b@b");
		inputRecord2.put("joinedOn", 6L);
		inputRecord2.put("resignedOn", 7L);
		inputRecord2.put("lastTransitionOn", 8L);
		inputRecord2.put("createdAt", 9L);
		inputRecord2.put("updatedAt", 10L);
		inputRecord2.put("locale", "C");
		inputRecord2.put("mgmtLevel", "D");
		inputRecord2.put("mgmtTransitionOn", 11L);
		inputRecord2.put("settingsEntityId", 16L);
		inputRecord2.put("settingsSettingId", 17L);
		inputRecord2.put("settingsSettingValue", "G");
		inputRecord2.put("settingsDateCreated", 18L);
		inputRecord2.put("settingsDateModified", 19L);
		inputRecord2.put("writeAccessLevel", "F");
		
		inputRecord3.put("txn", 1L);
		inputRecord3.put("membershipId", 2L);
		inputRecord3.put("memberId", 3L);
		inputRecord3.put("anetId", 4L);
		inputRecord3.put("entityId", 5L);
		inputRecord3.put("anetType", "A");
		inputRecord3.put("isPrimaryAnet", "Y");
		inputRecord3.put("state", "CA");
		inputRecord3.put("contactEmail", "b@b");
		inputRecord3.put("joinedOn", 6L);
		inputRecord3.put("resignedOn", 7L);
		inputRecord3.put("lastTransitionOn", 8L);
		inputRecord3.put("createdAt", 9L);
		inputRecord3.put("updatedAt", 10L);
		inputRecord3.put("locale", "C");
		inputRecord3.put("mgmtLevel", "D");
		inputRecord3.put("mgmtTransitionOn", 11L);
		inputRecord3.put("settingsEntityId", 20L);
		inputRecord3.put("settingsSettingId", 21L);
		inputRecord3.put("settingsSettingValue", "H");
		inputRecord3.put("settingsDateCreated", 22L);
		inputRecord3.put("settingsDateModified", 23L);
		inputRecord3.put("writeAccessLevel", "F");

		

		List<GenericRecord> records = new ArrayList<GenericRecord>();
		records.add(inputRecord);
		records.add(inputRecord2);
		records.add(inputRecord3);
		
		GenericRecord outputRecord = new GenericData.Record(AnetMembersSchemaConverterMain.OUTPUT_SCHEMA);
		AnetMembersSchemaConverterMain.AnetMembersReducer reducer = new AnetMembersSchemaConverterMain.AnetMembersReducer();
		reducer.convertSchema(records, outputRecord, null);
		System.out.println("GenericRecord is : " + outputRecord);

		// Serialize the row
		byte[] serializedValue;
		try
		{
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			Encoder encoder = new BinaryEncoder(bos);
			GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(AnetMembersSchemaConverterMain.OUTPUT_SCHEMA);
			writer.write(outputRecord, encoder);
			serializedValue = bos.toByteArray();
		}
		catch(IOException ex)
		{
			throw new RuntimeException("Failed to serialize the Avro GenericRecord. GenericRecord was :(" + outputRecord + ")", ex);
		}
		
		System.out.println("Bytestream size :" + serializedValue.length);

		BinaryDecoder binaryDecoder = new BinaryDecoder(new ByteArrayInputStream(serializedValue));
				
		JsonDecoder jsonDecoder = new JsonDecoder(AnetMembersSchemaConverterMain.OUTPUT_SCHEMA, outputRecord.toString());

		SpecificDatumReader<AnetMembers_V7> reader = new SpecificDatumReader<AnetMembers_V7>(AnetMembersSchemaConverterMain.OUTPUT_SCHEMA,AnetMembers_V7.SCHEMA$);
		
		reader.read(anetMember,binaryDecoder);
		
		System.out.println("SpecificRecord is : " + anetMember);
		System.out.println("TXN is : " + anetMember.get(0));
		Assert.assertEquals("TXN Check", 1L, anetMember.get(0));
		Assert.assertEquals("membershipId Check", 2L, anetMember.get(1));
		Assert.assertEquals("memberId Check", 3L, anetMember.get(2));
		Assert.assertEquals("anetId Check", 4L, anetMember.get(3));
		Assert.assertEquals("entityId Check", 5L, anetMember.get(4));
		Assert.assertEquals("anetType Check", "A" , anetMember.get(5).toString());
		Assert.assertEquals("isPrimaryAnet Check", "Y", anetMember.get(6).toString());
		Assert.assertEquals("state Check", "CA" , anetMember.get(7).toString());
		Assert.assertEquals("contactEmail Check", "b@b" , anetMember.get(8).toString());
		Assert.assertEquals("joinedOn Check", 6L , anetMember.get(9));
		Assert.assertEquals("resignedOn Check", 7L, anetMember.get(10));
		Assert.assertEquals("lastTransitionOn Check", 8L, anetMember.get(11));
		Assert.assertEquals("createdAt Check", 9L , anetMember.get(12));
		Assert.assertEquals("updatedAt Check", 10L, anetMember.get(13));
		Assert.assertEquals("locale Check", "C" , anetMember.get(14).toString());
		Assert.assertEquals("mgmtLevel Check", "D" , anetMember.get(15).toString());
		Assert.assertEquals("mgmtTransitionOn Check", 11L, anetMember.get(16));
		List<settingT> anetSettings = ((List<settingT>)((SETTINGS_T)(anetMember.get(17))).get(0));
		Assert.assertEquals("1 settingsEntityId Check", 12L, anetSettings.get(0).get(0));
		Assert.assertEquals("1 settingsSettingId Check", 13L, anetSettings.get(0).get(1));
		Assert.assertEquals("1 settingsSettingValue Check", "E", anetSettings.get(0).get(2).toString());
		Assert.assertEquals("1 settingsDateCreated Check", 14L, anetSettings.get(0).get(3));
		Assert.assertEquals("1 settingsDateModified Check", 15L, anetSettings.get(0).get(4));
		Assert.assertEquals("2 settingsEntityId Check", 16L, anetSettings.get(1).get(0));
		Assert.assertEquals("2 settingsSettingId Check", 17L, anetSettings.get(1).get(1));
		Assert.assertEquals("2 settingsSettingValue Check", "G", anetSettings.get(1).get(2).toString());
		Assert.assertEquals("2 settingsDateCreated Check", 18L, anetSettings.get(1).get(3));
		Assert.assertEquals("2 settingsDateModified Check", 19L, anetSettings.get(1).get(4));
		Assert.assertEquals("3 settingsEntityId Check", 20L, anetSettings.get(2).get(0));
		Assert.assertEquals("3 settingsSettingId Check", 21L, anetSettings.get(2).get(1));
		Assert.assertEquals("3 settingsSettingValue Check", "H", anetSettings.get(2).get(2).toString());
		Assert.assertEquals("3 settingsDateCreated Check", 22L, anetSettings.get(2).get(3));
		Assert.assertEquals("3 settingsDateModified Check", 23L, anetSettings.get(2).get(4));
		Assert.assertEquals("writeAccessLevel Check", "F",  anetMember.get(18).toString());
	}
}
