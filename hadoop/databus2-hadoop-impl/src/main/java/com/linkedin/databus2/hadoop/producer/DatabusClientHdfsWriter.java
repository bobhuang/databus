package com.linkedin.databus2.hadoop.producer;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.DatabusHttpClientImpl.CheckpointPersistenceStaticConfig.ProviderType;
import com.linkedin.databus.client.consumer.AbstractDatabusStreamConsumer;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.util.ConfigLoader;

@SuppressWarnings("deprecation")
public class DatabusClientHdfsWriter
  extends Configured implements Tool
{

	public static String SCHEMA = "{ \"name\" : \"LiarMemberRelay\", \"doc\" : \"Auto-generated Avro schema for SY$LIAR_MEMBER_RELAY. Generated at Dec 02, 2010 02:56:56 PM PST\", \"type\" : \"record\",\"meta\" : \"dbFieldName=SY$LIAR_MEMBER_RELAY;\",\"namespace\" : \"com.linkedin.events.liar.memberrelay\",\"fields\" : [ {\"name\" : \"txn\",\"type\" : [ \"int\", \"null\" ],\"meta\" : \"dbFieldName=TXN;dbFieldPosition=0;\"}, {\"name\" : \"key\",\"type\" : [ \"int\", \"null\" ],\"meta\" : \"dbFieldName=KEY;dbFieldPosition=1;\"}, {\"name\" : \"eventId\",\"type\" : [ \"int\", \"null\" ],\"meta\" : \"dbFieldName=EVENT_ID;dbFieldPosition=2;\"}, { \"name\" : \"isDelete\", \"type\" : [ \"string\", \"null\" ],\"meta\" : \"dbFieldName=IS_DELETE;dbFieldPosition=3;\"} ]}";
	public static Schema SCHEMA$ = Schema.parse(SCHEMA);

	/**
	 * Checks to see if a specific port is available.
	 *
	 * @param port the port to check for availability
	 */
	public static boolean portAvailable(int port) {
	    if (port < 1200 || port > 65000) {
	        throw new IllegalArgumentException("Invalid start port: " + port);
	    }

	    ServerSocket ss = null;
	    DatagramSocket ds = null;
	    try {
	        ss = new ServerSocket(port);
	        ss.setReuseAddress(true);
	        ds = new DatagramSocket(port);
	        ds.setReuseAddress(true);
	        return true;
	    } catch (IOException e) {
	    } finally {
	        if (ds != null) {
	            ds.close();
	        }

	        if (ss != null) {
	            try {
	                ss.close();
	            } catch (IOException e) {
	                /* should not be thrown */
	            }
	        }
	    }

	    return false;
	}

	public static class Databus2HadoopClientReducer extends AvroReducer<Utf8, GenericRecord, GenericRecord>
	{
		@Override
		public void reduce(Utf8 key, Iterable<GenericRecord> values, AvroCollector<GenericRecord> collector, Reporter reporter)
				throws IOException
		{
			Iterator<GenericRecord> itr = values.iterator();
			while (itr.hasNext())
			{
				collector.collect(itr.next());
			}

		}
	}

	public static class Databus2HadoopClientMapper
	   implements Mapper<LongWritable, Text, AvroKey<Text>, AvroValue<GenericRecord>>
	{
		private boolean isFirst = true;
		private Databus2HadoopConsumer _consumer = null;
		private DatabusHttpClientImpl _client = null;

		@Override
		public void configure(JobConf conf)
		{
		}


		@Override
		public void close() throws IOException
		{
		}


		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<AvroKey<Text>, AvroValue<GenericRecord>> collector,
				Reporter reporter) throws IOException
		{
			if ( isFirst)
			{
				_consumer = new Databus2HadoopConsumer(collector);
				_client = createAndStartDbusClient(_consumer);
				try
				{
					Thread.sleep(1000 * 60);
				} catch (InterruptedException ie ) {
					ie.printStackTrace();
				}
				_client.shutdown();
			}
		}


		public DatabusHttpClientImpl createAndStartDbusClient(Databus2HadoopConsumer consumer)
		{
		    Properties clientProps = new Properties();
		    DatabusHttpClientImpl client = null;

		    int port = 12989;

		    while (! portAvailable(port))
		    {
		    	port++;
		    }


		    try
		    {
		    	clientProps.setProperty("client.checkpointPersistence.type", ProviderType.NONE.toString());
		    	clientProps.setProperty("client.container.jmx.rmiEnabled", "false");
		    	clientProps.setProperty("client.container.httpPort", "" + port);
		    	clientProps.setProperty("client.runtime.bootstrap.enabled", "false");
		    	clientProps.setProperty("client.runtime.relay(1).name", "esv4-app23.stg");
		    	clientProps.setProperty("client.runtime.relay(1).host", "esv4-app23.stg");
		    	clientProps.setProperty("client.runtime.relay(1).port", "11115");
		    	clientProps.setProperty("client.runtime.relay(1).sources", "com.linkedin.events.liar.memberrelay.LiarMemberRelay");
		    	clientProps.setProperty("client.connectionDefaults.eventBuffer.maxSize", "100000");
		    	clientProps.setProperty("client.connectionDefaults.pullerRetries.maxRetryNum", "9");
		    	clientProps.setProperty("client.connectionDefaults.pullerRetries.sleepIncFactor", "1.0");
		    	clientProps.setProperty("client.connectionDefaults.pullerRetries.sleepIncDelta", "1");
		    	clientProps.setProperty("client.connectionDefaults.pullerRetries.initSleep", "1");

		    	DatabusHttpClientImpl.Config clientConfBuilder = new DatabusHttpClientImpl.Config();
		    	ConfigLoader<DatabusHttpClientImpl.StaticConfig> configLoader =
		    			new ConfigLoader<DatabusHttpClientImpl.StaticConfig>("client.", clientConfBuilder);
		    	configLoader.loadConfig(clientProps);

		    	DatabusHttpClientImpl.StaticConfig clientConf = clientConfBuilder.build();
			    client = new DatabusHttpClientImpl(clientConf);
			    client.registerDatabusStreamListener(consumer, null, "com.linkedin.events.liar.memberrelay.LiarMemberRelay" );
			    client.start();
		    } catch (Exception ex) {
		    	ex.printStackTrace();
		    	System.err.println("Got Exception while creating the client !! " + ex);
		    }
		    return client;
		}



	}


	public static class Databus2HadoopConsumer extends AbstractDatabusStreamConsumer
	{
		OutputCollector<AvroKey<Text>, AvroValue<GenericRecord>> _collector;

		Schema _currSchema = null;
		String _currSource = null;
		GenericRecord _currRecord = null;

		Map<String, Schema> _sourceSchemaMap = new HashMap<String, Schema>();
		Map<Schema, GenericRecord> _reuseMap = new HashMap<Schema, GenericRecord>();


		public Databus2HadoopConsumer(OutputCollector<AvroKey<Text>, AvroValue<GenericRecord>> collector)
		{
			_collector = collector;
		}

		@Override
    public ConsumerCallbackResult onStartSource(String source, Schema sourceSchema)
		{
			_currSource = source;
			_currSchema = sourceSchema;

			_sourceSchemaMap.put(_currSource, _currSchema);
			_currRecord = _reuseMap.get(_currSchema);

			if ( null == _currRecord)
			{
				_currRecord = new GenericData.Record(_currSchema);
				_reuseMap.put(_currSchema, _currRecord);
			}

			return ConsumerCallbackResult.SUCCESS;
		}

		@Override
		public ConsumerCallbackResult onDataEvent(DbusEvent e,
												  DbusEventDecoder eventDecoder)
		{
			_currRecord = eventDecoder.getGenericRecord(e, _currRecord);

			try
			{
			  //Pair<Text,GenericRecord> value = new Pair<Text,GenericRecord>(_currSource, _currRecord);
			  _collector.collect(new AvroKey<Text>(new Text(_currSource)), new AvroValue<GenericRecord>(_currRecord));
			} catch (Exception io) {
			  io.printStackTrace();
			  return ConsumerCallbackResult.ERROR_FATAL;
			}

			return ConsumerCallbackResult.SUCCESS;
		}

		public void setCollector(OutputCollector<AvroKey<Text>, AvroValue<GenericRecord>> collector)
		{
			_collector = collector;
		}
	}

	@Override
	public int run(String[] args) throws Exception
	{
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		JobConf conf = new JobConf(getConf(), getClass());
		conf.setJobName("Databus Hadoop Pipeline ");
		conf.setNumMapTasks(1);


		AvroJob.setOutputSchema(conf, SCHEMA$);
		Schema stringSchema = Schema.create(Schema.Type.STRING);
		Schema pairSchema = Pair.getPairSchema(stringSchema, SCHEMA$);
		AvroJob.setMapOutputSchema(conf, pairSchema);
		AvroJob.setOutputSchema(conf, SCHEMA$);

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(AvroOutputFormat.class);

		//AvroJob.setInputSchema(conf, inputRecordSchema);;
		AvroJob.setReducerClass(conf, Databus2HadoopClientReducer.class);
		conf.setMapperClass(Databus2HadoopClientMapper.class);

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args)
		throws Exception
	{
		int exitCode = ToolRunner.run(new DatabusClientHdfsWriter(), args);
		System.exit(exitCode);
	}

}
