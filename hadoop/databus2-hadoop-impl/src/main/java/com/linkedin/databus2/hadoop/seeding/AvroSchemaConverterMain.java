package com.linkedin.databus2.hadoop.seeding;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.linkedin.databus2.schemas.utils.SchemaHelper;

public abstract class AvroSchemaConverterMain
	extends Configured implements Tool
{

	public interface PairCreator<K>
	{
		Pair<K,GenericRecord> createPair(K key, GenericRecord record);
	}

	public static class LongKeyPairCreator implements PairCreator<Long>
	{
		@Override
    public Pair<Long, GenericRecord> createPair(Long key,
				GenericRecord record)
		{
			return new Pair<Long, GenericRecord>(key, record);
		}

	}

	public static class Utf8KeyPairCreator implements PairCreator<Utf8>
	{
		@Override
    public Pair<Utf8, GenericRecord> createPair(Utf8 key,
				GenericRecord record)
		{
			return new Pair<Utf8, GenericRecord>(key, record);
		}

	}

	public static abstract class SchemaConverterMapper<K> extends AvroMapper<GenericRecord, Pair<K,GenericRecord>>
	{
		private final String _keyFieldName;
		private final PairCreator<K>_outputRecordCreator;

		public SchemaConverterMapper(String inputKeyFieldName, PairCreator<K> outputRecordCreator)
		{
			_keyFieldName = inputKeyFieldName;
			_outputRecordCreator = outputRecordCreator;
		}

		@Override
		public void map(GenericRecord record,
				AvroCollector<Pair<K,GenericRecord>> collector,
				Reporter reporter) throws IOException
		{
			try
			{
				K key = (K)record.get(_keyFieldName);
				collector.collect(_outputRecordCreator.createPair(key, record));
			} finally {
			}
		}
	}

	public static abstract class SchemaConverterReducer<K> extends AvroReducer<K, GenericRecord, GenericRecord>
	{

		private final String _outputSchemaStr;
		private final Schema _outputSchema;

		public SchemaConverterReducer(String outputSchemaStr)
		{
			_outputSchemaStr = outputSchemaStr;
			_outputSchema = Schema.parse(outputSchemaStr);
		}

		@Override
		public void reduce(K key, Iterable<GenericRecord> values, AvroCollector<GenericRecord> collector, Reporter reporter)
		{
			GenericRecord output = new GenericData.Record(_outputSchema);
			convertSchema(values,output,reporter);
			try {
				collector.collect(output);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		public abstract void convertSchema(Iterable<GenericRecord> inputRecords, GenericRecord output, Reporter reporter);

		protected boolean copyFixedFields(String[] inputFields, String[] outputFields, GenericRecord input, GenericRecord output, String primaryField)
		{
			if ((null != primaryField) && (null == input.get(primaryField)))
				return false;
			Schema inputSchema = input.getSchema();
			for (int i = 0; i < inputFields.length; i++)
			{
				Field f = inputSchema.getField(inputFields[i]);
				
	        	Schema s = (Type.UNION == f.schema().getType()) ?
	                    					SchemaHelper.unwindUnionSchema(f)   : f.schema();
				
	            Type t = s.getType();
	            
	            switch (t)
	            {
	               case STRING: 
	            		Object inputObj = input.get(inputFields[i]);
	            		if ( null != inputObj)
	            		{
	            			Utf8 inp = (Utf8)inputObj;
	            			Utf8 out = new Utf8(inp.toString());
	            			output.put(outputFields[i],out);
	            		} else {
	            			output.put(outputFields[i],inputObj);
	            		}
	            		break;
	            	default:
	    	            output.put(outputFields[i],input.get(inputFields[i]));
	    	            break;
	            }
			}
			return true;
		}
	}

	private final Schema inputRecordSchema;
	private final Schema keySchema;
	private final Schema outputSchema;
	private final Class<? extends SchemaConverterMapper> mapperClass;
	private final Class<? extends SchemaConverterReducer> reducerClass;



	public AvroSchemaConverterMain(Schema inputRecordSchema,
			Schema keySchema,
			Schema outputSchema,
			Class<? extends SchemaConverterMapper> mapperClass,
			Class<? extends SchemaConverterReducer> reducerClass)
	{
		super();
		this.inputRecordSchema = inputRecordSchema;
		this.keySchema = keySchema;
		this.outputSchema = outputSchema;
		this.mapperClass = mapperClass;
		this.reducerClass = reducerClass;
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
		conf.setJobName("Avro Schema Conversion");

		//conf.set("mapred.reduce.child.java.opts", "-Xms4096M -Xmx4096M -XX:MaxPermSize=512M -XX:+UseCompressedOops");
		//conf.set("mapred.map.child.java.opts", "-Xms2048M -Xmx2048M -XX:MaxPermSize=512M -XX:+UseCompressedOops");

		conf.setNumReduceTasks(1000);

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		Schema pairSchema = Pair.getPairSchema(keySchema, inputRecordSchema);

		conf.setInputFormat(AvroInputFormat.class);
		conf.setOutputFormat(AvroOutputFormat.class);

		AvroJob.setInputSchema(conf, inputRecordSchema);;
		AvroJob.setMapOutputSchema(conf, pairSchema);
		AvroJob.setOutputSchema(conf, outputSchema);
		AvroJob.setMapperClass(conf, mapperClass);
		AvroJob.setReducerClass(conf, reducerClass);

		System.out.println("Input Record Schema :" + inputRecordSchema);
		System.out.println("Output Record Schema :" + outputSchema);
		JobClient.runJob(conf);
		return 0;
	}
}
