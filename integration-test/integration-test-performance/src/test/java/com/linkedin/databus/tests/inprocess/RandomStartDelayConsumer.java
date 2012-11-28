package com.linkedin.databus.tests.inprocess;

import java.io.File;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.HashSet;

import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.Encoding;
import com.linkedin.databus.core.OffsetNotFoundException;
import com.linkedin.databus.core.ScnNotFoundException;
import com.linkedin.databus.core.util.Utils;
import com.linkedin.databus2.core.filter.DbusFilter;

public class RandomStartDelayConsumer extends DatabusDelayConsumer {

	private double scnVariance=1;

	public RandomStartDelayConsumer(DbusEventBuffer dbuf, StaticConfig config,
			WritableByteChannel writeChannel, Encoding encoding, Double scnVariance ) {
		super(dbuf, config, writeChannel, encoding);
		this.scnVariance = scnVariance;
	}

	public RandomStartDelayConsumer(DbusEventBuffer dbuf, StaticConfig config,
			File writeFile ) {
		super(dbuf, config, writeFile);
	}

	public RandomStartDelayConsumer(DbusEventBuffer dbuf, long sleepMinTime,
			long sleepMaxTime, int batchFetchMinSize, int batchFetchMaxSize,
			int numRetries, HashSet<Integer> sourceIds, File writeFile,
			Encoding encoding) {
		super(dbuf, sleepMinTime, sleepMaxTime, batchFetchMinSize,
				batchFetchMaxSize, numRetries, sourceIds, writeFile, encoding);
		// TODO Auto-generated constructor stub
	}

	public RandomStartDelayConsumer(DbusEventBuffer dbuf, long sleepMinTime,
			long sleepMaxTime, int batchFetchMinSize, int batchFetchMaxSize,
			int numRetries, DbusFilter dbusFilter, File writeFile,
			Encoding encoding) {
		super(dbuf, sleepMinTime, sleepMaxTime, batchFetchMinSize,
				batchFetchMaxSize, numRetries, dbusFilter, writeFile, encoding);
		// TODO Auto-generated constructor stub
	}

	public RandomStartDelayConsumer(DbusEventBuffer dbuf, long sleepMinTime,
			long sleepMaxTime, int batchFetchMinSize, int batchFetchMaxSize,
			int numRetries, WritableByteChannel writeChannel,
			DbusFilter dbusFilter, Encoding encoding) {
		super(dbuf, sleepMinTime, sleepMaxTime, batchFetchMinSize,
				batchFetchMaxSize, numRetries, writeChannel, dbusFilter,
				encoding);
		// TODO Auto-generated constructor stub
	}

    @Override
    public void run()
    {
    	long testStartTime = System.currentTimeMillis();

        _consumerRate.start();

        try {
            Thread.sleep(_sleepTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Checkpoint cp = new Checkpoint();
        cp.setFlexible();
        if ( null == _writeChannel)
        {
        	try
        	{
        		_writeChannel = Utils.openChannel(_writeFile, true);
        	}
        	catch (IOException e2) { e2.printStackTrace(); }

        	System.out.println("Dump File is : " + _writeFile);
        }

        long numEventsStreamed = 0;

        Long minScn = _relayBuffer.getMinScn();
        while(minScn == -1) {
        	try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        	minScn = _relayBuffer.getMinScn();
        }


        try
        {
          while (!_stopConsumption.get())
          {
            int currentBatchFetchSize = _batchFetchSize;
            Long lastScn = _relayBuffer.lastWrittenScn();
            minScn = _relayBuffer.getMinScn();
            Long startScn = (long) (minScn + ((lastScn - minScn) * this.scnVariance));
            Long windowScn = (long) (((lastScn - startScn + 1) * Math.random()) + startScn);
            try
            {
              //System.out.println("Max SCN : " + maxScn + " , Mix SCN : " + minScn + " and setting current window scn to : " + windowScn);
			  cp.setWindowScn(windowScn);
			  cp.setWindowOffset(-1);
			  cp.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
              long numEvents = _relayBuffer.streamEvents(cp, false, currentBatchFetchSize,
                                                         _writeChannel, _encoding, _dbusFilter);

              numEventsStreamed += numEvents;
              _consumerRate.ticks(numEvents);
              cp.checkPoint();
              _maxScnConsumed = cp.getWindowScn();
              try
              {
                Thread.sleep(_sleepTime);
              } catch (InterruptedException e1) { e1.printStackTrace(); }
            }
            catch (ScnNotFoundException e)
            {
            	System.out.println("Last SCN : " + lastScn + " , Min SCN : " + minScn + " start Scn : " + startScn + " window Scn : " + windowScn + " batch fetch size : " + currentBatchFetchSize);
            	System.out.println("Relay buffer stats: MinScn = " + _relayBuffer.getMinScn() + " ,last scn = " + _relayBuffer.lastWrittenScn());
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

              System.out.println("Waiting for " + _sleepTime  + "... scn not found !!!! : " + cp);
              try {
                  Thread.sleep(_sleepTime);
              } catch (InterruptedException e1) {
                  e1.printStackTrace();
              }
              cp.setFlexible(); // Allow Read from whatever is available now
           }
            catch (OffsetNotFoundException e)
            {
            	System.out.println("Last SCN : " + lastScn + " , Min SCN : " + minScn + " start Scn : " + startScn + " window Scn : " + windowScn + " batch fetch size : " + currentBatchFetchSize);
            	System.out.println("Relay buffer stats: MinScn = " + _relayBuffer.getMinScn() + " ,last scn = " + _relayBuffer.lastWrittenScn());
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

            	System.out.println("Waiting for " + _sleepTime  + "... offset not found !!!! : " + cp);
            	try {
            		Thread.sleep(_sleepTime);
            	} catch (InterruptedException e1) {
            		e1.printStackTrace();
            	}
            	cp.setFlexible(); // Allow Read from whatever is available now
            } 
           catch (RuntimeException ex)
           {
             ex.printStackTrace();
             _isRuntimeError = true;
             throw ex;
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

}
