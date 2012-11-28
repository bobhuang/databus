package com.linkedin.databus2.hadoop.seeding;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.util.Utf8;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

/*
 * Note: Currently not used !! Required only if the input to MySQL "Load infile" needs to binary. Currently, it is in Hex format. 
 */
public class MySQLRecordOutputFormat extends FileOutputFormat<Utf8, ByteBuffer> 
{
	public static final String COLUMN_DELIMITER = "\t";
	public static final String ROW_DELIMITER = "\n";

	protected static class MySQLRecordWriter implements RecordWriter<Utf8, ByteBuffer>
	{
		private DataOutputStream _out;
		
		public MySQLRecordWriter(DataOutputStream out)
		{
			_out = out;
		}
		
		@Override
		public synchronized void close(Reporter reporter) throws IOException 
		{
			if ( null != _out)
				_out.close();
		}

		@Override
		public synchronized void write(Utf8 key, ByteBuffer val) throws IOException 
		{	
			_out.writeBytes(key.toString());
			_out.writeBytes(COLUMN_DELIMITER);
			_out.writeBytes(new String(Hex.encodeHex(val.array())));
			_out.writeBytes(ROW_DELIMITER);
		}		
	}
	
	@Override
	public RecordWriter<Utf8, ByteBuffer> getRecordWriter(FileSystem ignored, JobConf job,
											  String name, Progressable progress) throws IOException 
	{
		Path file = FileOutputFormat.getTaskOutputPath(job, name);
	    FileSystem fs = file.getFileSystem(job);
	    FSDataOutputStream fileOut = fs.create(file, progress);
	    return new MySQLRecordWriter(fileOut);
	}

}
