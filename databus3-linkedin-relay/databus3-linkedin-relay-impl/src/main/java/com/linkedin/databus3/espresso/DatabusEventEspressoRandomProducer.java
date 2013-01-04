package com.linkedin.databus3.espresso;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
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
import com.linkedin.databus.core.data_model.LogicalPartition;
import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.util.DatabusEventRandomProducer;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus.core.util.RngUtils;
import com.linkedin.databus2.schemas.SchemaId;
import com.linkedin.databus2.schemas.SchemaRegistryService;


/**
 * @author pganti
 *
 */
public class DatabusEventEspressoRandomProducer extends DatabusEventRandomProducer {
	public static final String MODULE = DatabusEventRandomProducer.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);

	public DatabusEventEspressoRandomProducer(DbusEventBufferMult  dbuf,
			int              eventsPerSecond,
			long             durationInMilliseconds,
			List<IdNamePair> sources,
			SchemaRegistryService schemaRegistryService) {
		this(dbuf,eventsPerSecond,durationInMilliseconds,sources,schemaRegistryService,null);
	}

	public DatabusEventEspressoRandomProducer(DbusEventBufferMult dbuf,
			int eventsPerSecond,
			long durationInMilliseconds,
			List<IdNamePair> sources,
			SchemaRegistryService schemaRegistryService,
			DatabusEventRandomProducer.StaticConfig config) {

		super(dbuf, 2, eventsPerSecond, durationInMilliseconds, sources, config);
	}


	/**
	 * Generate one Espresso Event
	 * @return the event a array of bytes
	 */
	public static byte[] produceOneEspressoEvent(IEspressoSchema es)
	{
		try {
			ChannelBuffer cb = ChannelBuffers.dynamicBuffer(70);
			ChannelBufferOutputStream cbos = new ChannelBufferOutputStream(cb);

			Encoder encoder = new BinaryEncoder(cbos);

			// Populate data
			GenericRecord r = es.createDataPerSchema();

			GenericDatumWriter<GenericRecord> w = new GenericDatumWriter<GenericRecord>(es.getSchema());
			w.write(r, encoder);

			encoder.flush();

			return cbos.buffer().array();
		} catch (Exception e2) {
			LOG.error("Exception when forming espresso request from databus event"
					+ e2.getMessage(), e2);
		}
		return null;


	}

	public long produceNEspressoEvents(long startScn, long currentTime, int numberOfEvents, List<IdNamePair> sources, long keyMin,
									   long keyMax)
	throws IOException
	{
		final long endScn = startScn + 1;
		assert (endScn > startScn);

		// Generate for the first source only
		int srcIndex = 0;
		long maxScn = startScn;

		final IdNamePair pair = sources.get(srcIndex);

		final int lSourceId = pair.getId().intValue();
		final String lSourceName = pair.getName();
		final LogicalSource lSource = new LogicalSource(lSourceId, lSourceName);

		LOG.info("Getting DbusEventBuffer for lSource: " + lSource);

		// This is the partition number in the json file.
		final short lPartitionId = 0;
		final LogicalPartition lp = new LogicalPartition(lPartitionId);
		final short pPartitionId  = _dbusEventBuffer.getPhysicalPartition(lSourceId, lp).getId().shortValue();
		final DbusEventBufferAppendable buf = _dbusEventBuffer.getDbusEventBufferAppendable(lSource, lp);

		// Start events
		buf.startEvents();

		// Generate n events
		long key = 0;
		for (int i = 0; i < numberOfEvents; ++i) {
			// Always update the minKey
			key = keyMin;

			// Choose a desirable schema
			IEspressoSchema es = null;
			if (lSourceId == 19)
				es = new IdNamePairSchema();
			else
				es = new EmailSchema();

			// Obtain schema parameters / info
			SchemaId schemaId = es.getSchemaId();
			short srcId = es.getSrcId();
			byte[] value = produceOneEspressoEvent(es);

			// sample 1% of events for tracing
			boolean enableTracing = (RngUtils.randomPositiveShort()%100 <=1);

			// append to the event
			boolean retVal = buf.appendEvent(new DbusEventKey(key++), pPartitionId, lPartitionId, currentTime,
									   		 srcId, schemaId.getByteArray(), value, enableTracing, _statsCollector);
			assert (retVal);

			// update maxScn, srcIndex
			LOG.info("Produce:"+maxScn+", srcId=" + srcId);
		}
		maxScn++;
		// End events
		buf.endEvents(maxScn,_statsCollector);
		LOG.info("Produce:window:" + maxScn);
		assert (maxScn >= startScn);

		return maxScn;
	}

	@Override
	public long produceNRandomEvents(long startScn, long currentTime, int numberOfEvents, List<IdNamePair> sources, long keyMin,
									 long keyMax, int minLength, int maxLength, List<Schema> schemas)
	throws KeyTypeNotImplementedException
	{
		long maxScn = 0;
		try
		{
			maxScn = produceNEspressoEvents(startScn, currentTime, numberOfEvents,  sources, keyMin, keyMax);
		}
		catch (IOException e)
		{
			LOG.error("Got IOException while producing events !!",e);
		}
		return maxScn;
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

		return super.startGeneration(startScn, eventsPerSecond, durationInMilliseconds,
				numEventToGenerate, percentOfBufferToGenerate,
				keyMin, keyMax,
				sources, statsCollector);
	}

}
