package com.linkedin.databus.relay.member2;
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


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;

import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.DbusEventBufferMult;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.KeyTypeNotImplementedException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.util.DatabusEventRandomProducer;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus.core.util.RngUtils;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.schemas.NoSuchSchemaException;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.utils.Utils;


public class DatabusEventProfileRandomProducer extends DatabusEventRandomProducer {


  private static final String FULLY_QUALIFIED_PROFILE_EVENT_NAME = "com.linkedin.events.member2.profile.MemberProfile";
  private static final int MEMBER_PROFILE_SRC_ID = 40;

  private final SchemaRegistryService schemaRegistryService;

  public DatabusEventProfileRandomProducer(DbusEventBufferMult  dbuf,
		                                   int              eventsPerSecond,
		                                   long             durationInMilliseconds,
		                                   List<IdNamePair> sources,
		                                   SchemaRegistryService schemaRegistryService) {
	  this(dbuf,eventsPerSecond,durationInMilliseconds,sources,schemaRegistryService,null);
  }

  public DatabusEventProfileRandomProducer(DbusEventBufferMult dbuf,
                                           int eventsPerSecond,
                                           long durationInMilliseconds,
                                           List<IdNamePair> sources,
                                           SchemaRegistryService schemaRegistryService,
                                           DatabusEventRandomProducer.StaticConfig config) {

      //TODO (Medium) (DDSDBUS-71) : Fix start scn as a input?
      super(dbuf, 2, eventsPerSecond, durationInMilliseconds, sources, config);

      this.schemaRegistryService = schemaRegistryService;
	}

    public static GenericRecord produceOneProfileEvent(Schema s, int minLength, int maxLength)
    {
        //TODO (Medium) (DDSDBUS-72) : reduce "new"s
      GenericRecord r = new GenericData.Record(s);

	//      BinaryEncoder e = new BinaryEncoder(cbos);
	    	r.put("memberId", RngUtils.randomPositiveLong());
	    	r.put("createdDate", RngUtils.randomPositiveLong());
	    	r.put("modifiedDate", RngUtils.randomPositiveLong());
	    	r.put("xmlSchemaVersion", RngUtils.randomPositiveLong());
	    	r.put("updateVersion", RngUtils.randomPositiveLong());
	    	r.put("profileAuthKey", new org.apache.avro.util.Utf8(RngUtils.randomString(20)));
	    	r.put("xmlContentClob", new org.apache.avro.util.Utf8(RngUtils.randomString(1000)));

	    	GenericArray<GenericRecord> educationHistory = new GenericData.Array<GenericRecord>(0, s.getField("profEducations").schema());
	    	GenericRecord educationHistoryRecord = new GenericData.Record(s.getField("profEducations").schema().getElementType());
	    	//LOG.info(s.getField("educationHistory").schema().getElementType());
	    	educationHistoryRecord.put("profileEducationId", RngUtils.randomPositiveLong());
	    	educationHistoryRecord.put("memberId", RngUtils.randomPositiveLong());
	    	educationHistoryRecord.put("createdDate", RngUtils.randomPositiveLong());
	    	educationHistoryRecord.put("modifiedDate", RngUtils.randomPositiveLong());
	    	educationHistoryRecord.put("xmlSchemaVersion", RngUtils.randomPositiveLong());
	    	educationHistoryRecord.put("startMonthyear", RngUtils.randomPositiveLong());
	    	educationHistoryRecord.put("endMonthyear", RngUtils.randomPositiveLong());
	    	educationHistoryRecord.put("schoolId", RngUtils.randomPositiveLong());
	    	educationHistoryRecord.put("countryCode", new org.apache.avro.util.Utf8(RngUtils.randomString(5)));
	    	educationHistoryRecord.put("provinceCode", new org.apache.avro.util.Utf8(RngUtils.randomString(4)));

	    	// Randomize the length of xmlContent
            int rnd = RngUtils.randomPositiveShort();
            int length = minLength + rnd % (maxLength - minLength);
	    	educationHistoryRecord.put("xmlContent", new org.apache.avro.util.Utf8(RngUtils.randomString(length)));
	    	educationHistory.add(educationHistoryRecord);
	    	r.put("profEducations", educationHistory);
	    	GenericArray<GenericRecord> positionHistory = new GenericData.Array<GenericRecord>(0, s.getField("profPositions").schema());

	    	r.put("profPositions", positionHistory);
	    	GenericArray<GenericRecord> profileElements = new GenericData.Array<GenericRecord>(0, s.getField("profElements").schema());
	    	r.put("profElements", profileElements);

	    	return r;
    }

    public static byte[] serializeEvent(Schema s, GenericRecord r)
    {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      GenericDatumWriter<GenericRecord> w = new GenericDatumWriter<GenericRecord>(s);

      try {
        //TODO (DDSDBUS-73): make encoding parameterized
          Encoder e = new BinaryEncoder(bos);
//            Encoder e = new JsonEncoder(s, cbos);
          w.write(r, e);
          e.flush();
      } catch (IOException e2) {
          LOG.error("Got exception while encoding  !!", e2);
      }

      return bos.toByteArray();
    }

    @Override
    public long produceNRandomEvents(long startScn, long currentTime, int numberOfEvents, List<IdNamePair> sources, long keyMin, long keyMax, int minLength, int maxLength, List<Schema> schemas) throws KeyTypeNotImplementedException
    {
      Schema s = schemas.get(0);
      short srcId = MEMBER_PROFILE_SRC_ID;
      long maxScn = 0;
      try
      {
        maxScn = produceNProfileEvents(startScn, currentTime, numberOfEvents, keyMin, keyMax, srcId, s);
      }
      catch (IOException e)
      {
		  LOG.error("Got exception while producing events !!", e);
      }
      return maxScn;
    }

	public long produceNProfileEvents(long startScn,
	                                  long currentTime,
	                                  int numberOfEvents,
	                                  long keyMin,
	                                  long keyMax,
	                                  short srcId,
	                                  Schema schema) throws IOException {
		long endScn = startScn + 1 + (RngUtils.randomPositiveLong() % 100L); // random number between startScn and startScn + 100;
	    long scnDiff = endScn - startScn;
	    long maxScn = startScn;

	    // get the right buffer by id
	    short pPartitionId = _dbusEventBuffer.getPhysicalPartition(srcId).getId().shortValue();
	    DbusEventBufferAppendable buf = _dbusEventBuffer.getDbusEventBufferAppendable(srcId);
		buf.startEvents();
		assert(endScn > startScn);
		if (scnDiff < 0) {
		  LOG.info("endScn = " + endScn + " startScn = " + startScn);
		}

		byte[] schemaId = Utils.md5(schema.toString().getBytes());

		long startTime = System.nanoTime();
		long serializationTime = 0;
		long totalByteNum = 0;
		long totalAddTime = 0;

		for (int i = 0; i < numberOfEvents; ++i) {
		    long key = RngUtils.randomPositiveLong(keyMin, keyMax);
		    long scn = startScn + (RngUtils.randomPositiveLong() % scnDiff);
            GenericRecord r = produceOneProfileEvent(schema, _minLength, _maxLength); // 1K of row data
		    long serializeStartTS = System.nanoTime();
	        byte[] value = serializeEvent(schema, r);
	        totalByteNum += value.length;
            long serializeEndTS = System.nanoTime();
            serializationTime += (serializeEndTS - serializeStartTS);
	        short lPartitionId = (short) (key % Short.MAX_VALUE);
	        boolean enableTracing = (RngUtils.randomPositiveShort()%100 <=1);  // sample 1% of events for tracing
            long addStartTS = System.nanoTime();
	        assert(buf.appendEvent(new DbusEventKey(key), pPartitionId, lPartitionId, currentTime,
	                                   srcId, schemaId, value, enableTracing, _statsCollector));
            long addEndTS = System.nanoTime();
            totalAddTime += (addEndTS - addStartTS);
	        maxScn = Math.max(scn, maxScn);
		}
		buf.endEvents(maxScn, _statsCollector);
        long endTime = System.nanoTime();
        long totalTime = endTime - startTime;

        LOG.info(String.format("Produce: window: %d events: %d size: %d total time:%d\n time/event(us): serialization: %8.3g   add: %8.3g  total: %8.3g",
                               maxScn, numberOfEvents, (totalByteNum / numberOfEvents),
                               totalTime,
                               (serializationTime / (1000.0 * numberOfEvents)),
                               (totalAddTime / (1000.0 * numberOfEvents)),
                               (totalTime / (1000.0 * numberOfEvents) )));

		assert(maxScn >= startScn);
		return maxScn;
	}

	private void getSchemas()
	{
	  short srcId=0;

	  String schema;
	  Schema s = null;
	  try
	  {
	    schema = schemaRegistryService.fetchLatestSchemaByType(FULLY_QUALIFIED_PROFILE_EVENT_NAME);
	    s = Schema.parse(schema);
	  }
	  catch (NoSuchSchemaException e2)
	  {
		  LOG.error("Got exception while fetching latest Schema !!", e2);
	  }
	  catch (DatabusException e2)
	  {
		  LOG.error("Got exception while fetching latest Schema !!", e2);
	  }
	  if (_schemas == null)
	  {
	    _schemas = new ArrayList<Schema>();
	  }
	  _schemas.add(s);

	  for (IdNamePair namePair: _sources)
	  {
	    if (namePair.getName().equals(FULLY_QUALIFIED_PROFILE_EVENT_NAME)) {
	      srcId = namePair.getId().shortValue();
	    }
	  }

	  if ( (null == s) || ( srcId <= 0) )
	  {
	    // Error
	    LOG.error("Schema is NULL or srcId is not positive. Stopping Gen");
	    return;
	  }
	}

	@Override

	public boolean startGeneration(long startScn,
	                               int eventsPerSecond,
	                               long durationInMilliseconds,
	                               long numEventToGenerate,
	                               int percentOfBufferToGenerate,
	                               long keyMin,
	                               long keyMax,
	                               List<IdNamePair> sources,
	                               DbusEventsStatisticsCollector statsCollector) {
        getSchemas();   // get the schema
        return super.startGeneration(startScn, eventsPerSecond, durationInMilliseconds,
                                     numEventToGenerate, percentOfBufferToGenerate,
                                     keyMin, keyMax,
                                     sources, statsCollector);
	}

}
