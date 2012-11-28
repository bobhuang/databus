package com.linkedin.databus2.hadoop.seeding;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventInfo;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.DbusEventKey.KeyType;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus.core.KeyTypeNotImplementedException;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus2.schemas.SchemaId;

public abstract class DatabusBootstrapDBRowCreatorMapReduce
	extends Configured implements Tool
{
    public static final Logger LOG = Logger.getLogger(DatabusBootstrapDBRowCreatorMapReduce.class.getName());
    public static final String SEQ_CONF_NAME = "seq";


	public static abstract class AvroBasedMap extends AvroMapper<GenericRecord, Pair<Utf8,Utf8>>
	{
		public AvroBasedMap(Schema schema, short sourceId, String keyField)
		{
		  super();
			this.schema = schema;
			this.schemaId = SchemaId.forSchema(schema).getByteArray();
			this.keyFieldName = keyField;
			this.srcId = sourceId;
		}

		private final Schema schema;
		private final byte[] schemaId;
		private final String keyFieldName;
		private final short srcId;
		private final long timestampNanos = System.nanoTime();

		private static final int HUNDRED_KB_IN_BYTES =     100000;

		private final byte[] b = new byte[HUNDRED_KB_IN_BYTES];
		private final ByteBuffer _buf = ByteBuffer.wrap(b);

		@Override
		public synchronized void map(GenericRecord record,  AvroCollector<Pair<Utf8,Utf8>> collector, Reporter reporter) throws IOException
		{
		  long sequenceId = getConf().getLong(SEQ_CONF_NAME, 0);
			short logicalPartitionId = 0;
			short physicalPartitionId = 0;
			DbusEventKey eventKey = null;
			String keyStr = null;
			try
			{
				try {
					eventKey = new DbusEventKey(record.get(keyFieldName));
					keyStr = ((eventKey.getKeyType() == KeyType.LONG) ? "" + eventKey.getLongKey() : eventKey.getStringKey());
				} catch (UnsupportedKeyException e) {
					throw new RuntimeException(e);
				}

				// Serialize the row
				byte[] serializedValue;
				try
				{
					ByteArrayOutputStream bos = new ByteArrayOutputStream();
					Encoder encoder = new BinaryEncoder(bos);
					GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
					writer.write(record, encoder);
					serializedValue = bos.toByteArray();
				}
				catch(IOException ex)
				{
					throw new RuntimeException("Failed to serialize the Avro GenericRecord. GenericRecord was :(" + record + ")", ex);
				}


				DbusEventInfo eventInfo = new DbusEventInfo(DbusOpcode.UPSERT,
						                                    sequenceId,
						                                    physicalPartitionId,
						                                    logicalPartitionId,
						                                    timestampNanos,
						                                    srcId,
						                                    schemaId,
						                                    serializedValue,
						                                    false,
						                                    true);

				int numBytes = 0;
				try {
					numBytes = DbusEvent.serializeEvent(eventKey,_buf,eventInfo);
				} catch (KeyTypeNotImplementedException e) {
					throw new RuntimeException(e);
				}
				byte[] buf2 = new byte[numBytes];
				_buf.flip();
				_buf.get(buf2);
				String eventStrHex = new String(Hex.encodeHex(buf2));
				Pair<Utf8,Utf8> outputPair = new Pair<Utf8, Utf8>(keyStr,eventStrHex);
				collector.collect(outputPair);
			} finally {
				_buf.clear();
			}
		}
	}


	public static class NonAvroBasedReducer extends MapReduceBase
	implements Reducer<AvroKey<Utf8>,AvroValue<Utf8>, Utf8, Utf8>
	{

		private int counter = 0;

		@Override
		public void reduce(AvroKey<Utf8> k, Iterator<AvroValue<Utf8>> values,
				OutputCollector<Utf8, Utf8> collector, Reporter reporter)
						throws IOException
		{
			counter++;

			Utf8 key = k.datum();

			if (values.hasNext())
				collector.collect(key, values.next().datum());


			if (counter%50000 == 0)
			{
				reporter.setStatus("Processed :" + counter + " rows !! Last seen Key : " + key);
			}
		}
	}

	public int run(String[] args, String jobName, Schema schema, int numReducers, Class MapperClass)
			throws Exception
	{
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}


		JobConf conf = new JobConf(getConf(), getClass());
		conf.setJobName(jobName);

		conf.setNumReduceTasks(numReducers);
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		FileOutputFormat.setCompressOutput(conf, true);

		AvroJob.setInputSchema(conf, schema);
		Schema stringSchema = Schema.create(Schema.Type.STRING);
		Schema pairSchema = Pair.getPairSchema(stringSchema, stringSchema);
		AvroJob.setMapOutputSchema(conf, pairSchema);
		AvroJob.setOutputSchema(conf, pairSchema);

		conf.setOutputFormat(TextOutputFormat.class);
		conf.setInputFormat(AvroInputFormat.class);

		AvroJob.setMapperClass(conf, MapperClass);
		conf.setReducerClass(NonAvroBasedReducer.class);

		LOG.info("using sequence number: " + conf.get(SEQ_CONF_NAME));

		JobClient.runJob(conf);
		return 0;
	}




	protected static String arrayToString(String[] arrayStr)
	{
		if ((arrayStr == null ) || arrayStr.length < 0 )
			return null;

		StringBuilder builder = new StringBuilder();

		builder.append(arrayStr[0]);

		if ( arrayStr.length > 0 )
		{
			for (int i = 1; i < arrayStr.length; i++)
			{
				builder.append("\n").append(arrayStr[i]);
			}
		}
		return builder.toString();
	}
}
