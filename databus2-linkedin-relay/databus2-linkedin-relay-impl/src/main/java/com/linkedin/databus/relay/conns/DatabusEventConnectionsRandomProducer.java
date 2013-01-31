package com.linkedin.databus.relay.conns;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;

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


public class DatabusEventConnectionsRandomProducer extends DatabusEventRandomProducer {

  public static final String MODULE = DatabusEventConnectionsRandomProducer.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  public static final String FULLY_QUALIFIED_CONNECTIONS_EVENT_NAME = "com.linkedin.events.conns.Connections";
  public static final int CONNS_SRC_ID = 601;
  private final SchemaRegistryService schemaRegistryService;
  private Map<Short,Map<Long,Long>> _srcKeyScnMaps = null;
  private long _sumKey = 0;
  private long _sumScn = 0;
  private boolean _equalSumScnAndKeyDistribution;
  private final Random _eventRng;

  public DatabusEventConnectionsRandomProducer(DbusEventBufferMult dbuf,
                                           int eventsPerSecond,
                                           long durationInMilliseconds,
                                           List<IdNamePair> sources,
                                           SchemaRegistryService schemaRegistryService,
                                           DatabusEventRandomProducer.StaticConfig config) {

      //TODO (Medium) (DDSDBUS-65) : Fix start scn as a input?
      super(dbuf, 2, eventsPerSecond, durationInMilliseconds, sources, config);

      this.schemaRegistryService = schemaRegistryService;
      _eventRng = (config.getEventRngSeed() != -1) ? new Random(config.getEventRngSeed())
                                                   : new Random();

      _equalSumScnAndKeyDistribution = false;
      LOG.info("_generationPattern = " + _generationPattern);
      if (_generationPattern. compareTo("EqualSumScnAndKey") == 0)
      {
        _equalSumScnAndKeyDistribution = true;
        // set up key set
        _srcKeyScnMaps = new HashMap<Short,Map<Long,Long>>();
        for (int i=0; i< sources.size(); i++)
        {
          HashMap<Long,Long> srcKeyScnMap= new HashMap<Long,Long>();
          Short srcId = sources.get(i).getId().shortValue();
          _srcKeyScnMaps.put(srcId, srcKeyScnMap);
        }
        LOG.info("_generation" +
        		"Pattern = use EuqalSumeScnAndKey Distribution");
      }
      LOG.info("_generationPattern = " + _generationPattern);
	}

    public GenericRecord produceOneConnectionsEvent(Schema s, int minLength, int maxLength)
    {
      //TODO (Medium) (DDSDBUS-66) : reduce "new"s

          GenericRecord r = new GenericData.Record(s);

          r.put("key", RngUtils.randomString(_eventRng, 30));
          r.put("sourceId",  RngUtils.randomPositiveLong(_eventRng, 0, 1000000000));
          r.put("destId", RngUtils.randomPositiveLong(_eventRng, 0, 1000000000));
          r.put("active", "Y");
          r.put("modifiedDate", RngUtils.randomPositiveLong(_eventRng, 0, 1000000000000L));
          r.put("createDate", RngUtils.randomPositiveLong(_eventRng, 0, 1000000000000L));
          r.put("txn", RngUtils.randomPositiveLong(_eventRng));

          return r;
    }

    public static byte[] convertToBytes(GenericRecord r)
    {
      ChannelBuffer cb = ChannelBuffers.dynamicBuffer(1000);
      ChannelBufferOutputStream cbos = new ChannelBufferOutputStream(cb);

      GenericDatumWriter<GenericRecord> w = new GenericDatumWriter<GenericRecord>(r.getSchema());
      try {
        //TODO (DDSDBUS-67): make encoding parameterized
          Encoder e = new BinaryEncoder(cbos);
//            Encoder e = new JsonEncoder(s, cbos);
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
      Schema s = schemas.get(0);
      short srcId = CONNS_SRC_ID;
      long maxScn = 0;
      try
      {
        maxScn = produceNConnectionsEvents(startScn, currentTime, numberOfEvents, keyMin, keyMax, srcId, s);
      }
      catch (IOException e)
      {
		  LOG.error("Got exception while producing N events!!", e);
      }
      return maxScn;
    }

    public ArrayList<Long> calculateKeysInAWindow(long endScn,
                                             int numberOfEvents,
                                             long keyMin,
                                             long keyMax,
                                             short srcId)
    {
      ArrayList <Long> keysInWindow = new ArrayList<Long>(numberOfEvents);
      Map<Long,Long> srcKeyScnMap = _srcKeyScnMaps.get(srcId);
      boolean done = false;
      long sumKey = 0;

      while (!done)
      {
        keysInWindow.clear();
        long sumKeyDiff = 0;
        sumKey = 0;
        long keyDiff;
        long key;
        int i;
        for (i = 0; i < numberOfEvents; ++i)
        {
          if (i%2 == 0)
          {
            keyDiff = RngUtils.randomPositiveLong(1, endScn) - endScn;
          }
          else
          {
            keyDiff = RngUtils.randomPositiveLong(endScn, endScn * 2) - endScn;
          }
          if (i == numberOfEvents-1)
          {
            keyDiff =  -sumKeyDiff;   // make the sum 0
            key = keyDiff + endScn;
            if (key < 0)
            {
//              LOG.info("Calculate Key: Minus key: endScn = " + endScn + ", recalculate key = " + key);
              break;   // do it again
            }
          }
          key = keyDiff + endScn;
//          LOG.info("Calculate Key: endScn = " + endScn + ", i = " + i + ", key = " + key + ", sumkey = " + sumKey + ", sumKeyDiff = " + sumKeyDiff);
          if (srcKeyScnMap.containsKey(key) || keysInWindow.indexOf(key) !=-1 )  // has the key
          {
//            LOG.info("Calculate Key: endScn = " + endScn + ", duplicate key = " + key);
            if (i == numberOfEvents-1)
            {
//              LOG.info("Calculate Key: endScn = " + endScn + ", recalculate key = " + key);
              break;   // do it again
            }
            if (keysInWindow.indexOf(key) != -1)
            {
//              sumKeyDiff -= endScn;  // We will have one key less
              sumKeyDiff -= 0;  // We will have one key less
            }
            else
            {
              sumKeyDiff -= (endScn - srcKeyScnMap.get(key));  // this key will replace the previous key->scn
            }
          }
          else   // new key do the sum
          {
            sumKeyDiff += keyDiff;
            sumKey += key;
          }
          keysInWindow.add(key);
        }
        if (i == numberOfEvents)   // get out of the inner loop ok
        {
          done = true;
        }
      }
//      LOG.info("Calculate Key: endScn = " + endScn + ", sumKey = " + sumKey + ", endScn * numberOfEvents =" + endScn * numberOfEvents);
//      assert(sumKey == (endScn * numberOfEvents));     // should be the same
      for (Long key: keysInWindow)
      {
        long oldScn = 0;
        if (srcKeyScnMap.containsKey(key))
        {
          oldScn = srcKeyScnMap.get(key);
        }
        else
        {
          _sumKey += key;    // add the key
        }
        _sumScn += (endScn - oldScn);
        srcKeyScnMap.put(key, endScn);
      }
      LOG.info("==EventPattern: scn = " + endScn + ", _sumScn = " + _sumScn + ", _sumKey = " + _sumKey);
      assert(_sumKey == _sumScn);     // should be the same
      return keysInWindow;
    }

	public long produceNConnectionsEvents(long startScn,
	                                  long currentTime,
	                                  int numberOfEvents,
	                                  long keyMin,
	                                  long keyMax,
	                                  short srcId,
	                                  Schema schema) throws IOException {
	  long endScn;
	  if (_equalSumScnAndKeyDistribution)
	  {
	    endScn = startScn + numberOfEvents - 1; // end scn should be the same with max scn
	  }
	  else
	  {
	    long scnRng = (_config.getEventRngSeed() != -1) ? numberOfEvents
	                                                    : 1 + RngUtils.randomPositiveLong();
	    endScn = startScn + scnRng % (100L * numberOfEvents); // random number between startScn and startScn + 100;
	  }
	  // get the right buffer by srcId
	  LOG.info("getting DbusEventBuffer for source: " + srcId);
	  short pPartitionId = (short)1;
      short lPartitionId = (short)1;
	  DbusEventBufferAppendable buf = _dbusEventBuffer.getDbusEventBufferAppendable(srcId);
	    long scnDiff = endScn - startScn;
	    long maxScn = startScn;
		buf.startEvents();
		assert(endScn > startScn);
		if (scnDiff < 0) {
		  LOG.info("endScn = " + endScn + " startScn = " + startScn);
		}

		byte[] schemaId = Utils.md5(schema.toString().getBytes());

		String key;
		long scn;
		for (int i = 0; i < numberOfEvents; ++i) {
		  if (_equalSumScnAndKeyDistribution)
		  {
		    scn = startScn + i;
		  }
		  else
		  {
	        long diff = (_config.getEventRngSeed() != -1) ? i
                                                            : RngUtils.randomPositiveLong()  % scnDiff;
		    scn = startScn + diff;
		   }
		  GenericRecord r = produceOneConnectionsEvent(schema, _minLength, _maxLength);
		  key = r.get("key").toString();
		  byte[] value = convertToBytes(r);
		  boolean enableTracing = (_config.getEventRngSeed() != -1) ?
		      false :
		      (RngUtils.randomPositiveShort()%100 < 1);  // sample 1% of events for tracing if not generating a specific timeline
		  long timestamp = (_config.getEventRngSeed() != -1) ? _config.getEventRngSeed() : currentTime; //fake time or true time
		  assert(buf.appendEvent(new DbusEventKey(key), pPartitionId, lPartitionId, timestamp,
		                             srcId, schemaId, value,  enableTracing, _statsCollector));
		  maxScn = Math.max(scn, maxScn);
		  LOG.info("Produce: endScn = " + endScn + ", key=" + key);
		}
		if (_equalSumScnAndKeyDistribution)
		{
		  assert(endScn == maxScn);     // should be the same
		}
		buf.endEvents(maxScn,_statsCollector);
		LOG.info("Produce:window:" + maxScn);
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
	    schema = schemaRegistryService.fetchLatestSchemaByType(FULLY_QUALIFIED_CONNECTIONS_EVENT_NAME);
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
	    if (namePair.getName().equals(FULLY_QUALIFIED_CONNECTIONS_EVENT_NAME)) {
	      srcId = namePair.getId().shortValue();
	    }
	  }

	  if ( (null == s) || ( srcId <= 0) )
	  {
	    // Error
	    LOG.error(" Schema is NULL or srcId is not positive. Stopping Gen");
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
