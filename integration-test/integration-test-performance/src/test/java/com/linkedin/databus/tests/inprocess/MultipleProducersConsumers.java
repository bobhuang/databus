package com.linkedin.databus.tests.inprocess;

import static org.junit.Assert.assertEquals;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBufferMult;
import com.linkedin.databus.core.Encoding;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.DatabusEventRandomProducer;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus2.core.filter.SourceDbusFilter;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;


public class MultipleProducersConsumers
{
    private static final int TEN_MB_IN_BYTES =     10000000;
    private static final long ONE_GIGABYTE_IN_BYTES = 1000000000;
    private static final long FIFTEEN_GIGABYTE_IN_BYTES = 15 * ONE_GIGABYTE_IN_BYTES;
    public static final String MODULE = MultipleProducersConsumers.class.getName();
    public static final Logger LOG = Logger.getLogger(MODULE);
	private static final int DEFAULT_BUFFER_SIZE = 10; // IN GigaBytes
	private static final Double DEFAULT_SCN_VARIANCE = 1.0;

    private Map<Long,Thread> _consumerThreadMap;
    private Map<Long,ConsumerState> _consumerStateMap;
    private List<DbusEventBuffer>     _dbufList;
    private List<DatabusEventRandomProducer> _producerList;
    private List<DbusSCNEventListener> _producerListenerList;
    private List<SourceDbusFilter> _producerFilterList;
    private List<Boolean>     _producerListStarted;
    private long        _generationDuration;
    private DbusEventBuffer.Config  _bufferConfig = null;
    private HashSet<Integer> _sources;
    private Map<DbusInprocessConsumer,Integer> _consumerProducerMap;


    public MultipleProducersConsumers()
    {
    	_consumerThreadMap = new HashMap<Long,Thread>();
    	_consumerStateMap = new HashMap<Long, ConsumerState>();
    	_dbufList = new ArrayList<DbusEventBuffer>();
    	_producerList = new ArrayList<DatabusEventRandomProducer>();
    	_producerListStarted = new ArrayList<Boolean>();
    	_producerListenerList = new ArrayList<DbusSCNEventListener>();
    	_producerFilterList = new ArrayList<SourceDbusFilter>();
    	_consumerProducerMap = new HashMap<DbusInprocessConsumer, Integer>();
    }

    public void stopGeneration(int index)
       throws Exception
    {
    	if ( !_producerListStarted.get(index))
    		throw new Exception("Producer not started !!");

    	_producerList.get(index).stopGenerationAndWait();
    	_producerList.get(index).join();
    }

    public void suspendGeneration(int index)
       throws Exception
    {
    	if ( !_producerListStarted.get(index))
    		throw new Exception("Producer not started !!");

    	_producerList.get(index).suspendGeneration();
    }

    public boolean isProducerRunning(int index)
    {
    	if ( !_producerListStarted.get(index))
    		return false;

    	return ((null != _producerList) && (null != _producerList.get(index)) && (_producerList.get(index).checkRunning()));
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
    		int producerId = _consumerProducerMap.get(consumer);
    		long maxScnProduced = _producerListenerList.get(producerId).getMaxScnSeen();
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

    public String getEventProductionRate(int index)
       throws Exception
    {
    	if ( null == _producerList || null == _producerList.get(index))
    		throw new Exception("Producer hasnt started yet !!");

    	return _producerList.get(index).getRateOfProduction();
    }

    public double getProductionRate(int index)
    throws Exception
    {
      return _producerList.get(index).getProductionRate();
    }

    public int startProducer(String configFile)
       throws Exception
    {
    	Properties props = new Properties();
        props.load(new FileInputStream(configFile));
        return startProducer(props);
    }

    public int startProducer(Properties props)
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


        return startProducer(producerStaticConfig,bufferStaticConfig);
    }

    public int startProducer(Properties props, String allocationPolicy, Long bufferSize)
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
            _bufferConfig.setMaxSize(bufferSize);
            int scnIndexSize = (int) ((bufferSize) / 20); // 5 % of buffer size;
            _bufferConfig.setScnIndexSize(scnIndexSize);

            ConfigLoader<DbusEventBuffer.StaticConfig> bufferStaticConfigLoader =
                        new ConfigLoader<DbusEventBuffer.StaticConfig>("databus.relay.eventBuffer.",
                                                                        _bufferConfig);
            DbusEventBuffer.StaticConfig bufferStaticConfig =
                                          bufferStaticConfigLoader.loadConfig(props);


            return startProducer(producerStaticConfig,bufferStaticConfig);
        }

    public int startProducer(DatabusEventRandomProducer.StaticConfig producerStaticConfig,
                                DbusEventBuffer.StaticConfig bufferStaticConfig)
      throws Exception
    {
    	int index = 0;
        try
        {
              _generationDuration = producerStaticConfig.getDuration();
              _sources = new HashSet<Integer>();
              List<IdNamePair> producerSources = producerStaticConfig.getIdNameList();
              System.out.println("Adding source to producer !!");
              for (IdNamePair pair: producerSources)
              {
            	  _sources.add(pair.getId().intValue());
            	  System.out.println("Adding Source to producer :" + pair.getId().intValue());
              }

        	  SourceDbusFilter _producerFilter = new SourceDbusFilter(_sources);
        	  _producerFilterList.add(_producerFilter);

        	  // for testing create a PhysicalConfig object
        	  PhysicalSourceConfig pConf = new PhysicalSourceConfig(producerSources);
        	  DbusEventBufferMult _dbuf = new DbusEventBufferMult();
        	  DbusEventBuffer _singleEventBuffer = new DbusEventBuffer(bufferStaticConfig);
        	  _dbuf.addBuffer(pConf.build(), _singleEventBuffer);
        	  _dbufList.add(_singleEventBuffer);

        	  DbusSCNEventListener _producerListener = new DbusSCNEventListener(_producerFilter,1);
        	  _singleEventBuffer.addInternalListener(_producerListener);
        	  _producerListenerList.add(_producerListener);

         	  DatabusEventRandomProducer _producer = new DatabusEventRandomProducer(_dbuf,producerStaticConfig);

        	 _producer.setName("DatabusRandomProducer");
             _producerList.add(_producer);
             index = _producerList.size() - 1;

             // Fill buffer initially before starting producer thread
             _producer.fillBuffer(producerStaticConfig.getEventRate() * 3600);

    	     //Start _producer
    	     _producer.start();

    	     _producerListStarted.add(true);
        } catch (Exception ex) {
        	System.out.println("Got exception while starting consumer:" + ex.getMessage());
        	throw ex;
        }
        return index;
    }

    public long startConsumer(String configFile)
    	throws Exception
    {
      Properties props = new Properties();

      props.load(new FileInputStream(configFile));
      return startConsumer(props, DEFAULT_SCN_VARIANCE);
    }

    public long startConsumer(Properties props, Double scnVariance)
       throws Exception
    {

      RandomStartDelayConsumer.Config consumerConfig =
                                new RandomStartDelayConsumer.Config();

      ConfigLoader<RandomStartDelayConsumer.StaticConfig> consumerStaticConfigLoader =
               new ConfigLoader<RandomStartDelayConsumer.StaticConfig>("databus.relay.randomConsumer.", consumerConfig);
      RandomStartDelayConsumer.StaticConfig consumerStaticConfig =
                                   consumerStaticConfigLoader.loadConfig(props);

      return startConsumer(consumerStaticConfig, scnVariance);

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


    public long startConsumer(RandomStartDelayConsumer.StaticConfig consumerStaticConfig, Double scnVariance)
    	throws Exception
    {
    	int _producerIndex = 0;
    	for (Boolean startedFlag : _producerListStarted) {
    		if (startedFlag)
    			break;
    		_producerIndex++;
    	}
    	if (_producerIndex==_producerListStarted.size())
    		throw new Exception("Consumer cannot start before some producer is started");

        long id = -1;

        try
        {
          RandomStartDelayConsumer consumer  = null;

          consumer = new RandomStartDelayConsumer(_dbufList.get(_producerIndex),
        		  consumerStaticConfig,
        		  new NullByteChannel(),
        		  Encoding.JSON,
        		  scnVariance);

          Thread consumerThread = new Thread(consumer);
          id = consumerThread.getId();
          consumerThread.setName("RandomStartDelayConsumer_" + id);
          _consumerThreadMap.put(id, consumerThread);

          List<DbusInprocessConsumer> consumers = new ArrayList<DbusInprocessConsumer>();
          consumers.add(consumer);
          ConsumerState consumerState = new ConsumerState(id);
          consumerState.setEndToEnd(false);
          consumerState.setConsumers(consumers);
          _consumerStateMap.put(id, consumerState);

          // Map consumer thread to producer
          _consumerProducerMap.put(consumer, _producerIndex);

          //Start Thread
          consumerThread.start();

        } catch ( Exception ex) {
        	System.out.println("Got exception while starting consumer:" + ex.getMessage());
        	throw ex;
        }
        return id;
    }

    public static void compareProducerConsumer(String testMsg,
    		                                   FileWriterDbusEventListener producer,
    		                                   DbusInprocessConsumer        consumer)
    {

    	long maxScnProduced = producer.maxScnSeen();
    	long maxScnConsumed = consumer.maxScnConsumed();

    	System.out.println("MaxScnProduced: " + maxScnProduced + ",MaxScnConsumed:" + maxScnConsumed);
    	assertEquals("MaxScn Seen check",maxScnProduced, maxScnConsumed);

    	System.out.println("PASS :" + testMsg);
    }

    public long getProducerDuration()
    {
      return _generationDuration;
    }
}
