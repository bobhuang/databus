package com.linkedin.databus2.hadoop.seeding.anets;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.events.anet.Anets_V6;
import com.linkedin.events.anet.DATABUS_GEO_T;
import com.linkedin.events.anet.SETTINGS_T;
import com.linkedin.events.anet.settingT;

public class TestAnetsSchemaConverterMain 
{
	@Test
	public void testOneRecordConversion()
		throws Exception
	{
		GenericRecord inputRecord = new GenericData.Record(AnetsSchemaConverterMain.INPUT_SCHEMA);
		Anets_V6 anetMember = new Anets_V6();
		
		inputRecord.put("txn", 1L);
		inputRecord.put("key", 2L);
		inputRecord.put("anetId", 3L);
		inputRecord.put("anetType", "A");
		inputRecord.put("entityId", 4L);
		inputRecord.put("state", "CA");
		inputRecord.put("name", "bob");
		inputRecord.put("locale", "en-us");
		inputRecord.put("createdAt", 5L);
		inputRecord.put("updatedAt", 6L);
		inputRecord.put("urlKey", "D");
		inputRecord.put("privateKey", "E");
		inputRecord.put("ownerId", 7L);
		inputRecord.put("shortDescription", "F");
		inputRecord.put("description", "G");
		inputRecord.put("largeLogoId", "H");
		inputRecord.put("smallLogoId", "X");
		inputRecord.put("vanityUrl", "I");
		inputRecord.put("category", "J");
		inputRecord.put("otherCategory", "K");
		inputRecord.put("contactEmail", "b@b");
		inputRecord.put("homeSite", "L");
		inputRecord.put("country", "US");
		inputRecord.put("postalCode", "94089");
		inputRecord.put("geoPostalCode", "94043");
		inputRecord.put("regionCode", 8);
		inputRecord.put("geoPlaceCode", "M");
		inputRecord.put("geoPlaceMaskCode", "N");
		inputRecord.put("latitudeDeg", 9.0f);
		inputRecord.put("longitudeDeg", 10.0f);
		inputRecord.put("anetSize", 11L);
		inputRecord.put("settingsEntityId", 12L);
		inputRecord.put("settingsSettingId", 13L);
		inputRecord.put("settingsSettingValue", "E");
		inputRecord.put("settingsDateCreated", 14L);
		inputRecord.put("settingsDateModified", 15L);
		inputRecord.put("parentAnetId", 16L);

		List<GenericRecord> records = new ArrayList<GenericRecord>();
		records.add(inputRecord);

		GenericRecord outputRecord = new GenericData.Record(AnetsSchemaConverterMain.OUTPUT_SCHEMA);
		AnetsSchemaConverterMain.AnetsReducer reducer = new AnetsSchemaConverterMain.AnetsReducer();
		reducer.convertSchema(records, outputRecord, null);
		System.out.println("GenericRecord is : " + outputRecord);

		// Serialize the row
		byte[] serializedValue;
		try
		{
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			Encoder encoder = new BinaryEncoder(bos);
			GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(AnetsSchemaConverterMain.OUTPUT_SCHEMA);
			writer.write(outputRecord, encoder);
			serializedValue = bos.toByteArray();
		}
		catch(IOException ex)
		{
			throw new RuntimeException("Failed to serialize the Avro GenericRecord. GenericRecord was :(" + outputRecord + ")", ex);
		}

		System.out.println("Bytestream size :" + serializedValue.length);

		BinaryDecoder binaryDecoder = new BinaryDecoder(new ByteArrayInputStream(serializedValue));

		JsonDecoder jsonDecoder = new JsonDecoder(AnetsSchemaConverterMain.OUTPUT_SCHEMA, outputRecord.toString());

		SpecificDatumReader<Anets_V6> reader = new SpecificDatumReader<Anets_V6>(AnetsSchemaConverterMain.OUTPUT_SCHEMA,Anets_V6.SCHEMA$);

		reader.read(anetMember,binaryDecoder);

		System.out.println("SpecificRecord is : " + anetMember);
		Assert.assertEquals( 1L, anetMember.get(0),"TXN Check");
		Assert.assertEquals( 2L, anetMember.get(1),"Key Check");
		Assert.assertEquals(3L, anetMember.get(2),"anetId Check");
		Assert.assertEquals("A", anetMember.get(3).toString(),"anetType Check");
		Assert.assertEquals(4L, anetMember.get(4),"entityId Check");
		Assert.assertEquals( "CA" , anetMember.get(5).toString(),"state Check");
		Assert.assertEquals( "bob" , anetMember.get(6).toString(),"Name Check");
		Assert.assertEquals( "en-us" , anetMember.get(7).toString(),"Locale Check");
		Assert.assertEquals( 5L , anetMember.get(8),"createdAt Check");
		Assert.assertEquals( 6L, anetMember.get(9),"updatedAt Check");
		Assert.assertEquals("D" , anetMember.get(10).toString(),"urlKey");
		Assert.assertEquals("E" , anetMember.get(11).toString(),"privateKey");
		Assert.assertEquals(7L, anetMember.get(12),"ownerId Check");
		Assert.assertEquals( "F" , anetMember.get(13).toString(),"shortDescription");
		Assert.assertEquals( "G" , anetMember.get(14).toString(),"description");
		Assert.assertEquals( "H" , anetMember.get(15).toString(),"largeLogo");
		Assert.assertEquals( "X" , anetMember.get(16).toString(),"smallLogo");
		Assert.assertEquals( "I" , anetMember.get(17).toString(),"vanityUrl");
		Assert.assertEquals( "J" , anetMember.get(18).toString(),"category");
		Assert.assertEquals( "K" , anetMember.get(19).toString(),"otherCategory");
		Assert.assertEquals( "b@b" , anetMember.get(20).toString(),"contactEmail Check");
		Assert.assertEquals( "L" , anetMember.get(21).toString(),"homesite");
		
		DATABUS_GEO_T geo = (DATABUS_GEO_T)anetMember.get(22);
		Assert.assertEquals( "US" , geo.get(0).toString(),"country");
		Assert.assertEquals( "94089" , geo.get(1).toString(),"postalCode");
		Assert.assertEquals( "94043", geo.get(2).toString(),"geoPostalCode");
		Assert.assertEquals( 8 , geo.get(3),"regionCode");
		Assert.assertEquals( "M" , geo.get(4).toString(),"geoPlaceCode");
		Assert.assertEquals( "N" , geo.get(5).toString(),"geoPlaceMaskCode");
		Assert.assertEquals( 9.0f , geo.get(6),"latitude");
		Assert.assertEquals( 10.0f , geo.get(7),"longitude");
		
		Assert.assertEquals( 11L , anetMember.get(23),"anetSize");
		
		List<settingT> anetSettings = ((List<settingT>)((SETTINGS_T)(anetMember.get(24))).get(0));
		Assert.assertEquals( 12L, anetSettings.get(0).get(0),"settingsEntityId Check");
		Assert.assertEquals(13L, anetSettings.get(0).get(1),"settingsSettingId Check");
		Assert.assertEquals("E", anetSettings.get(0).get(2).toString(),"settingsSettingValue Check");
		Assert.assertEquals( 14L, anetSettings.get(0).get(3),"settingsDateCreated Check");
		Assert.assertEquals(15L, anetSettings.get(0).get(4),"settingsDateModified Check");
		
		Assert.assertEquals( 16L , anetMember.get(25),"parentAnetId");
	}

}
