package com.linkedin.databus.relay.liar;
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


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;

import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBufferMult;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.KeyTypeNotImplementedException;
import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.util.DatabusEventRandomProducer;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus.core.util.RngUtils;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.schemas.NoSuchSchemaException;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.utils.Utils;



/**
 * @author dzhang
 *
 */
public class DatabusEventLiarRandomProducer extends DatabusEventRandomProducer {
  public static final String MODULE = DatabusEventRandomProducer.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private static final String FULLY_QUALIFIED_LIAR_JOB_RELAY_EVENT_NAME = "com.linkedin.events.liar.jobrelay.LiarJobRelay";
  private static final String FULLY_QUALIFIED_LIAR_MEMBER_RELAY_EVENT_NAME = "com.linkedin.events.liar.memberrelay.LiarMemberRelay";
  private static final short LIAR_JOB_RELAY_SRC_ID = 20;
  private static final short LIAR_MEMBER_RELAY_SRC_ID = 21;
  private final SchemaRegistryService schemaRegistryService;
  private static final String[] _fullyQualifiedEventNames = {FULLY_QUALIFIED_LIAR_JOB_RELAY_EVENT_NAME, FULLY_QUALIFIED_LIAR_MEMBER_RELAY_EVENT_NAME};
  public static final short[] _liarSrcIdList = {LIAR_JOB_RELAY_SRC_ID, LIAR_MEMBER_RELAY_SRC_ID};

  public DatabusEventLiarRandomProducer(DbusEventBufferMult  dbuf,
		                                   int              eventsPerSecond,
		                                   long             durationInMilliseconds,
		                                   List<IdNamePair> sources,
		                                   SchemaRegistryService schemaRegistryService) {
	  this(dbuf,eventsPerSecond,durationInMilliseconds,sources,schemaRegistryService,null);
  }

  public DatabusEventLiarRandomProducer(DbusEventBufferMult dbuf,
                                           int eventsPerSecond,
                                           long durationInMilliseconds,
                                           List<IdNamePair> sources,
                                           SchemaRegistryService schemaRegistryService,
                                           DatabusEventRandomProducer.StaticConfig config) {

      //TODO (Medium) (DDSDBUS-68) : Fix start scn as a input?
      super(dbuf, 2, eventsPerSecond, durationInMilliseconds, sources, config);

      this.schemaRegistryService = schemaRegistryService;
	}

    /**
     * Generate One Liar Event
     *     min, max Length are not used
     *     key in the event and key in the value field are not the same for random generator
     *     Currently avro schema for liar and bizfollow has key as int
     * @return the event a array of bytes
     */
    public static byte[] produceOneLiarEvent(int srcIndex, Schema s, int minLength, int maxLength, long k)
    {
      //TODO (Medium) (DDSDBUS-69) : reduce "new"s
      ChannelBuffer cb = ChannelBuffers.dynamicBuffer(50);
      ChannelBufferOutputStream cbos = new ChannelBufferOutputStream(cb);

      GenericDatumWriter<GenericRecord> w = new GenericDatumWriter<GenericRecord>(s);

  //      BinaryEncoder e = new BinaryEncoder(cbos);
          GenericRecord r = new GenericData.Record(s);

          int key = (int)k;
          r.put("key", key);
          r.put("eventId", key);
          r.put("isDelete", RandomUtils.nextBoolean() ? "Y" : "N");
          if (srcIndex == 0) {    // state is only on the job_relay table
            r.put("state", new org.apache.avro.util.Utf8(RngUtils.randomString(1)));
          }
          try {
            //TODO (DDSDBUS-70): make encoding parameterized
              Encoder e = new BinaryEncoder(cbos);
//                Encoder e = new JsonEncoder(s, cbos);
              w.write(r, e);
              e.flush();
          } catch (IOException e2) {
    		  LOG.error("Got exception while encoding !!", e2);
          }

      return cbos.buffer().array();
    }

    @Override
    public long produceNRandomEvents(long startScn, long currentTime, int numberOfEvents, List<IdNamePair> sources, long keyMin, long keyMax, int minLength, int maxLength, List<Schema> schemas) throws KeyTypeNotImplementedException
    {
      long maxScn = 0;
      try
      {
        maxScn = produceNLiarEvents(startScn, currentTime, numberOfEvents, keyMin, keyMax, sources, schemas);
      }
      catch (IOException e)
      {
    	  LOG.error("Got IOException while producing events !!",e);

      }
      return maxScn;
    }

	public long produceNLiarEvents(long startScn,
	                                  long currentTime,
	                                  int numberOfEvents,
	                                  long keyMin,
	                                  long keyMax,
	                                  List<IdNamePair> sources,
	                                  List<Schema> schemas) throws IOException {
      int numSources = sources.size();
      int srcIndex = 0;
      Schema schema;

	  long endScn = startScn + 1 + (RngUtils.randomPositiveLong() % 100L); // random number between startScn and startScn + 100;
	  long scnDiff = endScn - startScn;
	  long maxScn = startScn;

	  // get the right buffer - pick any source id - they all should be mapped to the same buf
	  IdNamePair pair = sources.get(0);
	  LogicalSource lSource = new LogicalSource(pair.getId().intValue(), pair.getName());
	  LOG.info("getting DbusEventBuffer for lSource: " + lSource);
	  short pPartitionId  = _dbusEventBuffer.getPhysicalPartition(lSource.getId()).getId().shortValue();
	  DbusEventBuffer buf = _dbusEventBuffer.getDbusEventBuffer(lSource);

	  buf.startEvents();
	  assert (endScn > startScn);
	  if (scnDiff < 0) {
	    LOG.info("endScn = " + endScn + " startScn = " + startScn);
	  }

	  ArrayList<byte[]> schemaIdList = new ArrayList<byte[]>(2);
	  for (int i = 0; i< numSources; i++)
	  {
	    schema = schemas.get(i);
	    schemaIdList.add(Utils.md5(schema.toString().getBytes()));
	  }

	  long key = 0;
	  for (int i = 0; i < numberOfEvents; ++i) {
	    if (srcIndex == 0)   // keep pk/fk relationship
	    {
	      key = RngUtils.randomPositiveLong(keyMin, keyMax);
	    }
	    long scn = startScn + (RngUtils.randomPositiveLong() % scnDiff);
	    schema = schemas.get(srcIndex);
	    short srcId = _liarSrcIdList[srcIndex];

	    byte[] value = produceOneLiarEvent(srcIndex, schema, _minLength, _maxLength, key); // 1K of row data


	    short lPartitionId = (short) (key % Short.MAX_VALUE);
	    boolean enableTracing = (RngUtils.randomPositiveShort()%100 <=1);  // sample 1% of events for tracing
	    buf.appendEvent(new DbusEventKey(key), pPartitionId, lPartitionId, currentTime,
	                               srcId, schemaIdList.get(srcIndex), value,
	                                            enableTracing, _statsCollector);
	    maxScn = Math.max(scn, maxScn);
	    srcIndex = (srcIndex+1) % numSources;
	    LOG.info("Produce:"+maxScn+", srcId=" + srcId);
	  }
	  buf.endEvents(maxScn,_statsCollector);
	  LOG.info("Produce:window:" + maxScn);
	  assert (maxScn >= startScn);
	  return maxScn;
	}

	private void getSchemas()
	{
	  short srcId=0;
	  int numSources = _liarSrcIdList.length;
	  int srcInd;

	  String schema;
	  Schema s;
	  for (srcInd=0; srcInd < numSources; srcInd++)
	  {
	    s = null;
	    try
	    {
	      schema = schemaRegistryService.fetchLatestSchemaByType(_fullyQualifiedEventNames[srcInd]);
	      s = Schema.parse(schema);
	    }
	    catch (NoSuchSchemaException e2)
	    {
	      LOG.error("Got NoSuchSchemaException while fetching schema",e2);
	    }
	    catch (DatabusException e2)
	    {
		  LOG.error("Got InternalException while fetching schema",e2);
	    }
	    if (_schemas == null)
	    {
	      _schemas = new ArrayList<Schema>();
	    }
	    _schemas.add(s);

	    for (IdNamePair namePair: _sources)
	    {
	      if (namePair.getName().equals(_fullyQualifiedEventNames[srcInd])) {
	        srcId = namePair.getId().shortValue();
	      }
	    }

	    if ( (null == s) || ( srcId <= 0) )
	    {
	      // Error
	      LOG.error("ERROR: Schema is NULL or srcId is not positive. Stopping Gen");
	      return;
	    }
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

  public static String[] getFullyQualifiedEventNames()
  {
    String[] result = new String[_fullyQualifiedEventNames.length];
    System.arraycopy(_fullyQualifiedEventNames, 0, result, 0, _fullyQualifiedEventNames.length);
    return result;
  }

}
