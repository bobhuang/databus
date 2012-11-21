package com.linkedin.databus.tests.inprocess;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.Pipe;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBuffer.QueuePolicy;
import com.linkedin.databus.core.DbusEventBufferMult;
import com.linkedin.databus.core.Encoding;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.DatabusEventRandomProducer;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus2.core.filter.DbusFilter;
import com.linkedin.databus2.core.filter.SourceDbusFilter;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;


public class RandomProducerConsumer
{
    private static final int TEN_MB_IN_BYTES =     10000000;
    private static final long ONE_GIGABYTE_IN_BYTES = 1000000000;
    private static final long FIFTEEN_GIGABYTE_IN_BYTES = 15 * ONE_GIGABYTE_IN_BYTES;
    public static final String MODULE = RandomProducerConsumer.class.getName();
    public static final Logger LOG = Logger.getLogger(MODULE);

    private Map<Long,Thread> _consumerThreadMap;
    private Map<Long,ConsumerState> _consumerStateMap;
    private DbusEventBufferMult     _dbuf;
    private DbusEventBuffer _singleEventBuffer;
    private DatabusEventRandomProducer _producer;
    private FileWriterDbusEventListener _producerListener;
    private DbusFilter  _producerFilter;
    private boolean     _producerStarted;
    private long        _generationDuration;
    private DbusEventBuffer.Config  _bufferConfig = null;
    private HashSet<Integer> _sources;

    public RandomProducerConsumer()
    {
    	_consumerThreadMap = new HashMap<Long,Thread>();
    	_consumerStateMap = new HashMap<Long, ConsumerState>();
    	_producerStarted = false;
    }

    public void stopGeneration()
       throws Exception
    {
    	if ( !_producerStarted)
    		throw new Exception("Producer not started !!");

    	_producer.stopGenerationAndWait();
    	_producer.join();
    }

    public void suspendGeneration()
       throws Exception
    {
    	if ( !_producerStarted)
    		throw new Exception("Producer not started !!");

    	_producer.suspendGeneration();
    }

    public boolean isProducerRunning()
    {
    	if ( !_producerStarted)
    		return false;

    	return ((null != _producer) && (_producer.checkRunning()));
    }

    public String getConsumerWriteFile(long id)
       throws Exception
    {
    	if (_consumerStateMap.containsKey(id))
    	{
    		DbusInprocessConsumer consumer =  _consumerStateMap.get(id).getConsumers().get(0);
    		return consumer.getDumpFile().getPath();
    	}
    	throw new Exception("Consumer with Id " + id + " doesnt exist");
    }


    public void testConsumer(Long id)
        throws Exception
    {
    	if (_consumerStateMap.containsKey(id))
    	{
        	DbusInprocessConsumer consumer = _consumerStateMap.get(id).getConsumers().get(0);
    		testConsumer(consumer);
    	} else {
    		throw new Exception("Consumer with id:" + id + " not found");
    	}
    }

    public void testConsumer(DbusInprocessConsumer consumer)
       throws Exception
    {
    	File producerFile = _producerListener.getWriteFile();

    	File expConsumerFile = getTempFile();
    	copyEventsForSources(producerFile,expConsumerFile,consumer.getSourceIds());
    	System.out.println("ProducerFile :" + expConsumerFile.getPath() + ",ConsumerFile:" + consumer.getDumpFile().getPath());
    	boolean success = compareFiles(expConsumerFile,consumer.getDumpFile());
    	System.out.println("Test Success:" + success);

    	if (!success)
    		throw new Exception("Consumer Test failed !!");
    }

    public void copyEventsForSources(File inputFile, File writeFile, HashSet<Integer> srcIds)
    {
    	BufferedReader iStream = null;
    	BufferedWriter writer  = null;
    	try
    	{
    	  iStream = new BufferedReader(new FileReader(inputFile));
    	  writer   = new BufferedWriter(new FileWriter(writeFile));
      	  JsonFactory jsonFactory = new JsonFactory();

    	  String line = null;
    	  while ((line = iStream.readLine()) != null)
    	  {
    		 // System.out.println("Line:" + line);
    		  JsonParser jp = jsonFactory.createJsonParser(line.getBytes());

    		  while (jp.nextToken() != null)
    		  {
    			  JsonToken token = jp.getCurrentToken();

    			  if ( (token != null ) && token.equals(JsonToken.FIELD_NAME))
    			  {
    			     String name  =  jp.getCurrentName();
    			     if ((name != null) && (name.equalsIgnoreCase("srcId")))
    			     {
    			    	 jp.nextToken();
    			    	 int src = jp.getIntValue();
    			         //System.out.println("Key:[" + name + "],Value: [" + src + "]");
       				  	 if ( srcIds.contains(src))
       				  	 {
       				  		writer.write(line);
       				  		writer.write("\n");
       				  		break;
       				  	 }
    			     } else if ((name != null) && (name.equalsIgnoreCase("endOfPeriod"))) {
    			    	 jp.nextToken();
    			    	 boolean eop = jp.getBooleanValue();
    			    	 if (eop)
    			    	 {
        				  	writer.write(line);
           				  	writer.write("\n");
           				  	break;
    			    	 }
    			     }
    			  }
    		  }
    		  jp.close();
    	  }
    	} catch (Exception ex) {
    		System.out.println("Got Exception :" + ex.getMessage());
    		ex.printStackTrace();
    	} finally {
    		try
    		{
    		   iStream.close();
    		   writer.close();
    		} catch (IOException io) {

    		}
    	}
    }

    public void trySleep(long duration)
    {
      try
      {
        Thread.sleep(duration);
      } catch (InterruptedException ie) {
        //
      }
    }

    /*
     * @return Returns the number of ScnNotFoundErrors during its lifetime
     */
    public int stopConsumption(Long id, boolean waitForCompletion, long waitTimeMs)
       throws Exception
    {
    	int numErrors = 0;

    	if ( waitForCompletion )
    	{
    	  long numRetries = waitTimeMs/1000 + 1;
          int i = 0;
    	  while ( ! isConsumerDone(id))
    	  {
    	    i++;

    	    if ( i > numRetries )
    	      throw new RuntimeException("Consumer :" + id + " didnt reach MaxSCN !!");

    	    trySleep(1000);
    	  }
    	}

    	if (_consumerStateMap.containsKey(id))
    	{
    	    ConsumerState state = _consumerStateMap.get(id);

    		if ( state.isEndToEnd())
    		{
    			List<DbusInprocessConsumer> consumers = state.getConsumers();
    			consumers.get(1).stopConsumptionAndWait();
    			trySleep(10000);
                state.getPipeProducer().stopPipeAndWait();
                trySleep(10000);
                consumers.get(0).stopConsumptionAndWait();
    		}

        	DbusInprocessConsumer consumer = _consumerStateMap.get(id).getConsumers().get(0);
    	    numErrors = consumer.getNumScnNotFoundErrorsSeen();
    		Thread t = _consumerThreadMap.get(id);

    		if ( null == t)
    			return numErrors;

    	    consumer.stopConsumptionAndWait();
    		t.join();
    		_consumerThreadMap.remove(id);
    	} else {
    		throw new Exception("No Consumer found with id:" + id);
    	}
    	return numErrors;
    }

    public double getConsumerRate(long id)
    {
      double consumerRate = -1;
      if (_consumerStateMap.containsKey(id))
      {
        DbusInprocessConsumer consumer = _consumerStateMap.get(id).getConsumers().get(0);
        consumerRate = consumer.getRate();
      }
      return consumerRate;
    }

    public boolean isConsumerRuntimeError(long id)
    {
      boolean consumerRuntimeError = false;
      if (_consumerStateMap.containsKey(id))
      {
        DbusInprocessConsumer consumer = _consumerStateMap.get(id).getConsumers().get(0);
        consumerRuntimeError = consumer.isRuntimeError();
      }
      return consumerRuntimeError;
    }

    public int getConsumerScnNotFoundErrors(long id)
    {
      int consumerErrors = -1;
      if (_consumerStateMap.containsKey(id))
      {
        DbusInprocessConsumer consumer = _consumerStateMap.get(id).getConsumers().get(0);
        consumerErrors = consumer.getNumScnNotFoundErrorsSeen();
      }
      return consumerErrors;
    }

    public List<Long> getConsumerScnNotFoundErrorOccurences(long id)
    {
      List<Long> errorOccurences = new ArrayList<Long>();

      int consumerErrors = -1;
      if (_consumerStateMap.containsKey(id))
      {
          DbusInprocessConsumer consumer = _consumerStateMap.get(id).getConsumers().get(0);
    	  errorOccurences = consumer.getErrorOccurences();
      }
      return errorOccurences;
    }

    public boolean isConsumerDone(Long id)
          throws Exception
    {
    	if (_consumerStateMap.containsKey(id))
    	{
			Thread t = _consumerThreadMap.get(id);
			if (null == t)
				return true;

    		DbusInprocessConsumer consumer =  _consumerStateMap.get(id).getConsumers().get(0);
    		boolean done = isConsumerDone(consumer);
    		if (done)
    		{
              // wait till the thread dies
              Thread thread = _consumerThreadMap.get(id);
              if (null != thread)
                  thread.join();

              _consumerThreadMap.remove(id);
    		}
    		return done;
    	}
    	return false;
    }

    public boolean isConsumerDone(DbusInprocessConsumer consumer)
    throws Exception
    {
    		long maxScnProduced = _producerListener.getMaxScnSeen();
    		/*
    		Iterator<Integer> itr = consumer.getSourceIds().iterator();

    		while (itr.hasNext())
    		{
    			long scn = _producerListener.getMaxScnSeen(itr.next());
    			maxScnProduced = Math.max(maxScnProduced, scn);
    		}
    		*/

    		long maxScnConsumed = consumer.maxScnConsumed();
    		System.out.println("maxScnConsumed:" + maxScnConsumed + ",maxScnProduced:" + maxScnProduced);
    		if (maxScnProduced < maxScnConsumed)
    		{
    			throw new Exception(
    					"maxScnProduced less than maxScnConsumed. Produced:" +
    					 maxScnProduced + ", Consumed:" + maxScnConsumed);
    		}

    		if (maxScnProduced == maxScnConsumed)
    		{
    			System.out.println("Consumer reached maxScnProduced");

    			consumer.stopConsumption();

    			return true;
    		}
    	return false;
    }

    public String getEventProductionRate()
       throws Exception
    {
    	if ( null == _producer)
    		throw new Exception("Producer hasnt started yet !!");

    	return _producer.getRateOfProduction();
    }

    public double getProductionRate()
    throws Exception
    {
      return _producer.getProductionRate();
    }

    public String startProducer(String configFile)
        throws Exception
    {
      return startProducer(configFile,true);
    }

    public String startProducer(String configFile, boolean persist)
       throws Exception
    {
    	Properties props = new Properties();
        props.load(new FileInputStream(configFile));
        return startProducer(props,persist);
    }

    public String startProducer(Properties props, boolean persist)
        throws Exception
    {
        DatabusEventRandomProducer.Config producerConfig =
                                  new DatabusEventRandomProducer.Config();

        ConfigLoader<DatabusEventRandomProducer.StaticConfig> producerStaticConfigLoader =
                 new ConfigLoader<DatabusEventRandomProducer.StaticConfig>("databus.relay.randomProducer.", producerConfig);
        DatabusEventRandomProducer.StaticConfig producerStaticConfig =
                                     producerStaticConfigLoader.loadConfig(props);
        _bufferConfig = new DbusEventBuffer.Config();

        ConfigLoader<DbusEventBuffer.StaticConfig> bufferStaticConfigLoader =
                    new ConfigLoader<DbusEventBuffer.StaticConfig>("databus.relay.eventBuffer.",
                                                                    _bufferConfig);
        DbusEventBuffer.StaticConfig bufferStaticConfig =
                                      bufferStaticConfigLoader.loadConfig(props);


        return startProducer(producerStaticConfig,bufferStaticConfig,persist);
    }

    public String startProducer(Properties props, boolean persist, String allocationPolicy, int numBuffers)
            throws Exception
        {
            DatabusEventRandomProducer.Config producerConfig =
                                      new DatabusEventRandomProducer.Config();

            ConfigLoader<DatabusEventRandomProducer.StaticConfig> producerStaticConfigLoader =
                     new ConfigLoader<DatabusEventRandomProducer.StaticConfig>("databus.relay.randomProducer.", producerConfig);
            DatabusEventRandomProducer.StaticConfig producerStaticConfig =
                                         producerStaticConfigLoader.loadConfig(props);
            _bufferConfig = new DbusEventBuffer.Config();
            _bufferConfig.setAllocationPolicy(allocationPolicy);

            ConfigLoader<DbusEventBuffer.StaticConfig> bufferStaticConfigLoader =
                        new ConfigLoader<DbusEventBuffer.StaticConfig>("databus.relay.eventBuffer.",
                                                                        _bufferConfig);
            DbusEventBuffer.StaticConfig bufferStaticConfig =
                                          bufferStaticConfigLoader.loadConfig(props);


            return startProducer(producerStaticConfig,bufferStaticConfig,persist,numBuffers);
        }

    public String startProducer(DatabusEventRandomProducer.StaticConfig producerStaticConfig,
            DbusEventBuffer.StaticConfig bufferStaticConfig,
            boolean persist)
      throws Exception
    {
    	return startProducer (producerStaticConfig,bufferStaticConfig,persist,1);
    }


    public String startProducer(DatabusEventRandomProducer.StaticConfig producerStaticConfig,
                                DbusEventBuffer.StaticConfig bufferStaticConfig,
                                boolean persist, int numBuffers)
      throws Exception
    {
        try
        {
          if ( null == _producer)
          {
              _generationDuration = producerStaticConfig.getDuration();
              File producerFile = getTempFile();
              _sources = new HashSet<Integer>();
              List<IdNamePair> producerSources = producerStaticConfig.getIdNameList();
              System.out.println("Adding source to producer !!");
              for (IdNamePair pair: producerSources)
              {
            	  _sources.add(pair.getId().intValue());
            	  System.out.println("Adding Source to producer :" + pair.getId().intValue());
              }

        	  _producerFilter = new SourceDbusFilter(_sources);

        	  // for testing create a PhysicalConfig object
        	  PhysicalSourceConfig pConf = new PhysicalSourceConfig(producerSources);
        	  _dbuf = new DbusEventBufferMult();
        	  _singleEventBuffer = new DbusEventBuffer(bufferStaticConfig);
        	  _dbuf.addBuffer(pConf.build(), _singleEventBuffer);


        	  if ( persist )
        	  {
                _producerListener = new FileWriterDbusEventListener(producerFile,
                                                                  Encoding.JSON,
                                                                  _producerFilter,
                                                                  1); // First event on the producer needs to be skipped as it is not available for Consumers

                _singleEventBuffer.addInternalListener(_producerListener);
        	  }

             _producer = new DatabusEventRandomProducer(_dbuf,producerStaticConfig);
             _producer.setName("DatabusRandomProducer");

    	     //Start _producer
    	     _producer.start();
    	     _producerStarted = true;
    	  }
        } catch (Exception ex) {
        	System.out.println("Got exception while starting consumer:" + ex.getMessage());
        	throw ex;
        }

        String writeFile = null;
        if ( persist)
          writeFile = _producerListener.getWriteFile().getPath();

        return writeFile;
    }

    public long startConsumer(String configFile, boolean endToEnd)
        throws Exception
    {
    	return startConsumer(configFile,null,endToEnd);
    }

    public long startConsumer(String configFile, WritableByteChannel writeChannel, boolean endToEnd)
    	throws Exception
    {
      Properties props = new Properties();

      props.load(new FileInputStream(configFile));
      return startConsumer(props, writeChannel, endToEnd);
    }

    public long startConsumer(Properties props, WritableByteChannel writeChannel, boolean endToEnd)
       throws Exception
    {

      DatabusDelayConsumer.Config consumerConfig =
                                new DatabusDelayConsumer.Config();

      ConfigLoader<DatabusDelayConsumer.StaticConfig> consumerStaticConfigLoader =
               new ConfigLoader<DatabusDelayConsumer.StaticConfig>("databus.relay.randomConsumer.", consumerConfig);
      DatabusDelayConsumer.StaticConfig consumerStaticConfig =
                                   consumerStaticConfigLoader.loadConfig(props);

      return startConsumer(consumerStaticConfig,writeChannel,endToEnd);

    }


    /*
     * State Maintained for each started Consumers
     */
    public static class ConsumerState
    {
      private boolean               		_endToEnd;
      private List<DbusInprocessConsumer> 	_consumers;
      private DatabusPipeProducer   		_pipeProducer;
      private long                          _id;
      private DbusEventBuffer               _consumerEventBuffer;
      private List<Long>                    _threadIds;


      public ConsumerState(long id)
      {
    	  _id = id;
      }


      public List<Long> getAssociatedThreadIds() {
		return _threadIds;
      }


      public void setAssociatedThreadIds(List<Long> associatedThreadIds) {
		this._threadIds = associatedThreadIds;
      }


	public DbusEventBuffer getConsumerEventBuffer() {
		return _consumerEventBuffer;
      }


      public void setConsumerEventBuffer(DbusEventBuffer consumerEventBuffer) {
		this._consumerEventBuffer = consumerEventBuffer;
      }


	/*
       * @returns true if this consumer is a pipeline
       * PipeLine : RandomProducer -EventBuffer-> DelayConsumer -Pipe->PipeProducer -EventBuffer-> ClientConsumer
       */
      public boolean isEndToEnd() {
		return _endToEnd;
      }


      public long getId() {
		return _id;
	}


	public void setId(long id) {
		this._id = id;
	}


	public void setEndToEnd(boolean endToEnd) {
		this._endToEnd = endToEnd;
      }

      public List<DbusInprocessConsumer> getConsumers() {
		return _consumers;
      }

      public void setConsumers(List<DbusInprocessConsumer> consumers) {
		this._consumers = consumers;
      }

      public DatabusPipeProducer getPipeProducer() {
		return _pipeProducer;
      }

      public void setPipeProducer(DatabusPipeProducer pipeProducer) {
		this._pipeProducer = pipeProducer;
      }
    }


    public long startConsumer(DatabusDelayConsumer.StaticConfig consumerStaticConfig,
    		                  WritableByteChannel writeChannel,
    		                  boolean end2End)
    	throws Exception
    {
    	if ( ! _producerStarted)
    		throw new Exception("Consumer cannot start before producer. (Will be allowed in future)");

        long id = -1;

        try
        {
          DatabusDelayConsumer consumer  = null;
          if ( ! end2End)
          {
        	  if ( null != writeChannel)
        	  {
        		  consumer = new DatabusDelayConsumer(_singleEventBuffer,
        				  				consumerStaticConfig,
        				  				writeChannel,
        				  				Encoding.JSON);


        	  } else {
        		  File consumerFile = getTempFile();

        		  consumer = new DatabusDelayConsumer(_singleEventBuffer,
        				  							consumerStaticConfig,
        				  							consumerFile);
        	  }

        	  Thread consumerThread = new Thread(consumer);
        	  id = consumerThread.getId();
        	  consumerThread.setName("DatabusDelayConsumer_" + id);
        	  _consumerThreadMap.put(id, consumerThread);

        	  List<DbusInprocessConsumer> consumers = new ArrayList<DbusInprocessConsumer>();
        	  consumers.add(consumer);
        	  ConsumerState consumerState = new ConsumerState(id);
        	  consumerState.setEndToEnd(false);
        	  consumerState.setConsumers(consumers);
        	  _consumerStateMap.put(id, consumerState);

        	  //Start Thread
        	  consumerThread.start();
          } else {
    		  File consumerFile = getTempFile();
        	  Pipe pipe = Pipe.open();
        	  pipe.source().configureBlocking(false);

        	  DatabusDelayConsumer relayConsumer = new DatabusDelayConsumer(_singleEventBuffer,
		  																consumerStaticConfig,
		  																pipe.sink(),
		  																Encoding.BINARY);
        	  _bufferConfig.setQueuePolicy(QueuePolicy.BLOCK_ON_WRITE.toString());
        	  DbusEventBuffer eventBuffer = new DbusEventBuffer(_bufferConfig.build());
        	  //eventBuffer.start(0);

        	  DatabusPipeProducer  producer = new DatabusPipeProducer(eventBuffer, pipe);
        	  DatabusClientConsumer clientConsumer = new DatabusClientConsumer(eventBuffer,consumerFile);
        	  clientConsumer.setSourceIds(_sources);

        	  List<DbusInprocessConsumer> consumers = new ArrayList<DbusInprocessConsumer>();
        	  consumers.add(clientConsumer); // Add ClientConsumer first !!
        	  consumers.add(relayConsumer);

        	  Thread relayConsumerThread = new Thread(relayConsumer);
        	  Thread clientConsumerThread = new Thread(clientConsumer);

        	  id = clientConsumerThread.getId();
        	  List<Long> associatedThreadIds = new ArrayList<Long>();
        	  associatedThreadIds.add(relayConsumerThread.getId());
        	  associatedThreadIds.add(producer.getId());
        	  associatedThreadIds.add(id);

        	  ConsumerState consumerState = new ConsumerState(id);
        	  consumerState.setEndToEnd(true);
        	  consumerState.setAssociatedThreadIds(associatedThreadIds);
        	  consumerState.setConsumerEventBuffer(eventBuffer);
        	  consumerState.setConsumers(consumers);
        	  consumerState.setId(id);
        	  consumerState.setPipeProducer(producer);

        	  clientConsumerThread.setName("ClientConsumer_" + id);
        	  relayConsumerThread.setName("RelayConsumer_" + id);

        	  _consumerThreadMap.put(producer.getId(),producer);
        	  _consumerThreadMap.put(id, clientConsumerThread);
        	  _consumerThreadMap.put(relayConsumerThread.getId(), relayConsumerThread);

        	  _consumerStateMap.put(id, consumerState);
              relayConsumerThread.start();
        	  producer.start();
              clientConsumerThread.start();

          }
        } catch ( Exception ex) {
        	System.out.println("Got exception while starting consumer:" + ex.getMessage());
        	throw ex;
        }
        return id;
    }

    public static File getTempFile()
    {
        File directory = new File(".");
        File tempFile = null;
        try
        {
            tempFile = File.createTempFile("test", ".dbus", directory);
        }
        catch (IOException e2)
        {
            // TODO Auto-generated catch block
            e2.printStackTrace();
        }
        return tempFile;
    }

    public static void compareProducerConsumer(String testMsg,
    		                                   FileWriterDbusEventListener producer,
    		                                   DbusInprocessConsumer        consumer)
    {

    	long maxScnProduced = producer.maxScnSeen();
    	long maxScnConsumed = consumer.maxScnConsumed();

    	System.out.println("MaxScnProduced: " + maxScnProduced + ",MaxScnConsumed:" + maxScnConsumed);
    	assertEquals("MaxScn Seen check",maxScnProduced, maxScnConsumed);

    	assertTrue(compareFiles(producer.getWriteFile(), consumer.getDumpFile()));

    	System.out.println("PASS :" + testMsg);
    }

    public static boolean compareFiles (File producerFile,
    		                     File consumerFile)
    {
    	BufferedReader prodReader = null;
    	BufferedReader conReader = null;
    	try
    	{
    		prodReader = new BufferedReader (new FileReader(producerFile));
    		conReader = new BufferedReader (new FileReader(consumerFile));

    		String line1 = prodReader.readLine();
    		String line2 = conReader.readLine();

    		while ( (line1 != null) && (line2 != null))
    		{
    			if (!(line1.equals(line2)))
    			{
    				System.out.println("Line1 :" + line1 +"\nLine2 :" + line2);
    				System.out.println("ProducerFile :" + producerFile + ",ConsumerFile :" + consumerFile);
    				return false;
    			}
    			line1 = prodReader.readLine();
    			line2 = conReader.readLine();
    		}

    		if (( line1 != null) || (line2 != null))
    		{
    			System.out.println("Number of lines did not match between files :"
    					              + producerFile + "," + consumerFile);
    			return false;
    		}

    		return true;
    	} catch (IOException io) {
    		io.printStackTrace();
    		System.out.println("Caught IOException while comparing files :"
    				            + producerFile + "," + consumerFile);
    		return false;
    	} finally {
    		try
    		{
    		  prodReader.close();
    		  conReader.close();
    		} catch (IOException ioe) {
    			System.out.println("Exception while closing..");
    		}
    	}
    }

    public long getProducerDuration()
    {
      return _generationDuration;
    }
}
