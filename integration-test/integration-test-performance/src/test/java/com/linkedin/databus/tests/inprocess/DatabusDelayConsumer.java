package com.linkedin.databus.tests.inprocess;

import java.io.File;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.Encoding;
import com.linkedin.databus.core.ScnNotFoundException;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.RateMonitor;
import com.linkedin.databus.core.util.RngUtils;
import com.linkedin.databus.core.util.Utils;
import com.linkedin.databus2.core.filter.DbusFilter;
import com.linkedin.databus2.core.filter.SourceDbusFilter;

public class DatabusDelayConsumer implements Runnable, DbusInprocessConsumer
{
    protected final DbusEventBuffer _relayBuffer;
    protected final long _sleepMinTime;
    protected final long _sleepMaxTime;
    protected final int _batchFetchMinSize;
    protected final int _batchFetchMaxSize;


    protected boolean _isRuntimeError = false;
    protected long _sleepTime = 0;
    protected int _batchFetchSize = 0;
    protected long  _maxScnConsumed=0;
    protected HashSet<Integer> _sourceIds;
    protected final DbusFilter _dbusFilter;
    protected final int        _numRetries;
    protected final Encoding   _encoding;

    protected       int            _numScnNotFoundErrors;
    protected final AtomicBoolean _stopConsumption = new AtomicBoolean(false);
    protected final CountDownLatch _consumerStopped;
    protected final RateMonitor    _consumerRate;
    protected       List<Long>   _erorOccurences;

    protected       WritableByteChannel _writeChannel;
    protected 	  File         _writeFile;

    //private       File

    public DatabusDelayConsumer(DbusEventBuffer 		dbuf,
    							StaticConfig    		config,
    							WritableByteChannel     writeChannel,
    							Encoding                encoding)
    {
    	this(dbuf,
       		 config.getMinSleepTime(),
       		 config.getMaxSleepTime(),
       		 config.getMinBatchSize(),
       		 config.getMaxBatchSize(),
       		 config.getNumRetries(),
       		 writeChannel,
       		 new SourceDbusFilter(config.getSources()),
       		 encoding);
    	_sourceIds = config.getSources();
    }

    public DatabusDelayConsumer(DbusEventBuffer dbuf,
                                StaticConfig    config,
                                File            writeFile)
    {
    	this(dbuf,
    		 config.getMinSleepTime(),
    		 config.getMaxSleepTime(),
    		 config.getMinBatchSize(),
    		 config.getMaxBatchSize(),
    		 config.getNumRetries(),
    		 config.getSources(),
    		 writeFile,
    		 Encoding.JSON);
    }


    public DatabusDelayConsumer(DbusEventBuffer dbuf,
            					long            sleepMinTime,
            					long            sleepMaxTime,
            					int             batchFetchMinSize,
            					int             batchFetchMaxSize,
            					int             numRetries,
            					HashSet<Integer>  sourceIds,
            					File            writeFile,
            					Encoding        encoding)
    {
    	this(dbuf,sleepMinTime, sleepMaxTime, batchFetchMinSize, batchFetchMaxSize,
    	      numRetries,new SourceDbusFilter(sourceIds),writeFile,encoding);
    	_sourceIds = sourceIds;
    }

    public DatabusDelayConsumer(DbusEventBuffer dbuf,
                                long            sleepMinTime,
                                long            sleepMaxTime,
                                int             batchFetchMinSize,
                                int             batchFetchMaxSize,
                                int             numRetries,
                                DbusFilter      dbusFilter,
                                File            writeFile,
                                Encoding        encoding)
    {
    	this(dbuf,sleepMinTime,sleepMaxTime,batchFetchMinSize,batchFetchMaxSize,numRetries,null,dbusFilter,encoding);
    	_writeFile = writeFile;

    }


    public DatabusDelayConsumer(DbusEventBuffer 	dbuf,
            					long            	sleepMinTime,
            					long            	sleepMaxTime,
            					int             	batchFetchMinSize,
            					int            		batchFetchMaxSize,
            					int             	numRetries,
            					WritableByteChannel writeChannel,
            					DbusFilter          dbusFilter,
            					Encoding            encoding)
    {
        _consumerRate = new RateMonitor(getClass().getName());
        _consumerStopped = new CountDownLatch(1);
        _relayBuffer = dbuf;
        _sleepMinTime = sleepMinTime;
        _sleepMaxTime = sleepMaxTime;
        _batchFetchMinSize = batchFetchMinSize;
        _batchFetchMaxSize = batchFetchMaxSize;
        _numRetries        = numRetries;
        _dbusFilter = dbusFilter;
        _writeChannel = writeChannel;
        _writeFile = null;
        _encoding = encoding;
        _erorOccurences = new ArrayList<Long>();

        _sleepTime =  _sleepMinTime +
                 RngUtils.randomPositiveLong()%
                                  (_sleepMaxTime - _sleepMinTime);

        _batchFetchSize = _batchFetchMinSize +
                 RngUtils.randomPositiveInt()%
                                  (_batchFetchMaxSize - _batchFetchMinSize);
    }

    /* (non-Javadoc)
     * @see com.linkedin.databus.tests.inprocess.DbusInprocessConsumer#getErrorOccurences()
     */
    @Override
    public List<Long> getErrorOccurences()
    {
    	return _erorOccurences;
    }

    /* (non-Javadoc)
     * @see com.linkedin.databus.tests.inprocess.DbusInprocessConsumer#getSourceIds()
     */
    @Override
    public HashSet<Integer> getSourceIds()
    {
    	return _sourceIds;
    }

    /* (non-Javadoc)
     * @see com.linkedin.databus.tests.inprocess.DbusInprocessConsumer#maxScnConsumed()
     */
    @Override
    public long maxScnConsumed() {
        return _maxScnConsumed;
    }

    /* (non-Javadoc)
     * @see com.linkedin.databus.tests.inprocess.DbusInprocessConsumer#stopConsumption()
     */
    @Override
    public void stopConsumption()
    {
      _stopConsumption.set(true);
    }

    /* (non-Javadoc)
     * @see com.linkedin.databus.tests.inprocess.DbusInprocessConsumer#getRate()
     */
    @Override
    public double getRate()
    {
      return _consumerRate.getRate();
    }

    /* (non-Javadoc)
     * @see com.linkedin.databus.tests.inprocess.DbusInprocessConsumer#stopConsumptionAndWait()
     */
    @Override
    public void stopConsumptionAndWait()
    {
      _stopConsumption.set(true);
      while ( true)
      {
        try
        {
          _consumerStopped.await();
          break;
        } catch ( InterruptedException ie) {
        }
      }
    }

    /* (non-Javadoc)
     * @see com.linkedin.databus.tests.inprocess.DbusInprocessConsumer#getDumpFile()
     */
    @Override
    public File getDumpFile()
    {
      return _writeFile;
    }

    /* (non-Javadoc)
     * @see com.linkedin.databus.tests.inprocess.DbusInprocessConsumer#getNumScnNotFoundErrorsSeen()
     */
    @Override
    public int getNumScnNotFoundErrorsSeen()
    {
      return _numScnNotFoundErrors;
    }

    @Override
    public void run()
    {
    	long testStartTime = System.currentTimeMillis();

        _consumerRate.start();

        try {
            Thread.sleep(_sleepTime);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

     //   ArrayList<InternalDatabusEventsListener> eventListeners = new ArrayList<InternalDatabusEventsListener>();
        Checkpoint cp = new Checkpoint();
        cp.setFlexible();
   //       eventListeners.add(cp);
        if ( null == _writeChannel)
        {
        	try
        	{
        		_writeChannel = Utils.openChannel(_writeFile, true);
        	}
        	catch (IOException e2)
        	{
        		// TODO Auto-generated catch block
        		e2.printStackTrace();
        	}

        	System.out.println("Dump File is : " + _writeFile);
        }

        int retry = 0;
        long numEventsStreamed = 0;

        try
        {
          while (!_stopConsumption.get())
          {
            int currentBatchFetchSize = _batchFetchSize;
            try
            {
             // _consumerRate.resume();
              long numEvents = _relayBuffer.streamEvents(cp, false, currentBatchFetchSize,
                                                         _writeChannel, _encoding, _dbusFilter);
              numEventsStreamed += numEvents;
              _consumerRate.ticks(numEvents);
             // _consumerRate.suspend();
              cp.checkPoint();
              _maxScnConsumed = cp.getWindowScn();
              //System.out.println(Thread.currentThread().getId() + " : Checkpoint is =" + cp);
              try
              {
                //System.out.println("Consumer Sleeping for :" + _sleepTime);
                Thread.sleep(_sleepTime);
              } catch (InterruptedException e1) {
                e1.printStackTrace();
              }
            }
            catch (RuntimeException ex)
            {
              ex.printStackTrace();
              _isRuntimeError = true;
              throw ex;
            }
            catch (Exception e)
            {
              _consumerRate.suspend();
              _numScnNotFoundErrors++;
              _erorOccurences.add((System.currentTimeMillis() - testStartTime)/1000);
              _erorOccurences.add(numEventsStreamed);

        	  if (_numScnNotFoundErrors > _numRetries)
        	  {
        		  System.out.println(Thread.currentThread().getId() + "Retry Max reached. Giving up ...");
        		  return;
        	  }

              e.printStackTrace();

              System.out.println("Waiting for " + _sleepTime  + " msec... scn not found !!!! : " + cp);
              try {
                  Thread.sleep(_sleepTime);
              } catch (InterruptedException e1) {
                  e1.printStackTrace();
              }
              cp.setFlexible(); // Allow Read from whatever is available now
           }
        }
        } finally {
          try
          {
            _writeChannel.close();
          } catch (IOException io) {
            io.printStackTrace();
          }
          _consumerRate.stop();
          _consumerStopped.countDown();
        }
     }

     /* (non-Javadoc)
     * @see com.linkedin.databus.tests.inprocess.DbusInprocessConsumer#isRuntimeError()
     */
    @Override
    public boolean isRuntimeError()
     {
       return _isRuntimeError;
     }

    public static class StaticConfig
    {
    	public StaticConfig(long minSleepTime, long maxSleepTime,
    			            int minBatchSize, int maxBatchSize,
    			            int numRetries, HashSet<Integer> sources)
    	{
    		this.minSleepTime = minSleepTime;
    		this.maxSleepTime = maxSleepTime;
    		this.minBatchSize = minBatchSize;
    		this.maxBatchSize = maxBatchSize;
    		this.numRetries   = numRetries;
    		this.sources = sources;
    		System.out.println("ConsumerConfig - minSleepTime:" + minSleepTime + ",maxSleepTime:" + maxSleepTime
    				          + ",minBatchSize:" + minBatchSize + ",maxBatchSize:" + maxBatchSize
    				          + ",numRetries:" + numRetries + ",sources:" + sources);
    	}

		public long getMinSleepTime() {
			return minSleepTime;
		}

		public long getMaxSleepTime() {
			return maxSleepTime;
		}
		public int getMinBatchSize() {
			return minBatchSize;
		}
		public int getMaxBatchSize() {
			return maxBatchSize;
		}
		public int getNumRetries() {
			return numRetries;
		}

    	public HashSet<Integer> getSources() {
			return sources;
		}

		private long minSleepTime;
    	private long maxSleepTime;
    	private int minBatchSize;
    	private int maxBatchSize;
    	private int numRetries;
		private HashSet<Integer> sources;
    }

    public static class Config implements ConfigBuilder<StaticConfig>
    {
    	public long getMinSleepTime() {
			return minSleepTime;
		}
		public void setMinSleepTime(long minSleepTime) {
			this.minSleepTime = minSleepTime;
		}
		public long getMaxSleepTime() {
			return maxSleepTime;
		}
		public void setMaxSleepTime(long maxSleepTime) {
			this.maxSleepTime = maxSleepTime;
		}
		public int getMinBatchSize() {
			return minBatchSize;
		}
		public void setMinBatchSize(int minBatchSize) {
			this.minBatchSize = minBatchSize;
		}
		public int getMaxBatchSize() {
			return maxBatchSize;
		}
		public void setMaxBatchSize(int maxBatchSize) {
			this.maxBatchSize = maxBatchSize;
		}
		public int getNumRetries() {
			return numRetries;
		}

		public void setNumRetries(int numRetries) {
			System.out.println("NumRetries: " + numRetries);
			this.numRetries = numRetries;
		}

		@Override
		public StaticConfig build() throws InvalidConfigException
		{
			HashSet<Integer> sourceIds = new HashSet<Integer>();

			if ( sources != null)
			{
				String[] srcIds = sources.split("[,]");
				for(int i = 0; i < srcIds.length; i++)
				{
					sourceIds.add(Integer.parseInt(srcIds[i]));
					System.out.println("DEBUG: SrcId:" + srcIds[i]);
				}

			}
			return new StaticConfig(minSleepTime,maxSleepTime, minBatchSize,
					                maxBatchSize,numRetries,sourceIds);
		}

    	public String getSources() {
			return sources;
		}
		public void setSources(String sources) {
			System.out.println("Setting sources:" + sources);
			this.sources = sources;
		}

		protected long minSleepTime;
    	protected long maxSleepTime;
    	protected int minBatchSize;
    	protected int maxBatchSize;
    	protected int numRetries;
		protected String sources;
    }
}


