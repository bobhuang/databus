package com.linkedin.databus.tests.inprocess;

import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBuffer.DbusEventIterator;
import com.linkedin.databus.core.DbusEventInternalWritable;
import com.linkedin.databus.core.Encoding;
import com.linkedin.databus.core.util.RateMonitor;
import com.linkedin.databus.core.util.Utils;
import java.io.File;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class DatabusClientConsumer implements DbusInprocessConsumer, Runnable
{

  private long                      _maxScnConsumed;
  private HashSet<Integer>          _sourceIds;
  private DbusEventBuffer           _eventBuffer;
  private final File                _writeFile;
  private final WritableByteChannel _writeChannel;
  
  private final AtomicBoolean _stopConsumption = new AtomicBoolean(false);
  private final CountDownLatch _consumerStopped;
  private final RateMonitor    _consumerRate;
  private boolean              _consumerRuntimeError;
  private HashSet<Integer>     _srcIds;
  
  public DatabusClientConsumer(DbusEventBuffer eventBuffer, File writeFile)
    throws Exception
  {
    _maxScnConsumed  = 0;
    _eventBuffer = eventBuffer;
    _writeFile = writeFile;
    _consumerRate = new RateMonitor(getClass().getName());
    _consumerStopped = new CountDownLatch(1);
    _writeChannel = Utils.openChannel(_writeFile, true);
    _consumerRuntimeError = false;
  }
  
  @Override
  public void run()
  {
    try
    {
      _consumerRate.start();
      while (!_stopConsumption.get())
      {
        consumeEvents();
      }
      _consumerRate.stop();
    } catch ( Exception ex ) {
      ex.printStackTrace();
      _consumerRuntimeError = true;
    } finally {
      try
      {
        if ( null != _writeChannel )
          _writeChannel.close();
      } catch (IOException io) {
        io.printStackTrace();
      }
      _consumerStopped.countDown();
    }
  }

  private void consumeEvents()
  {
    DbusEventIterator itr = null;
    try
    {
      itr = _eventBuffer.acquireIterator("DatabusInprocessConsumer");
      
      while (!_stopConsumption.get() && !itr.hasNext())
      {
        itr.await(100,TimeUnit.MILLISECONDS); 
      }
      
      while (!_stopConsumption.get() && itr.hasNext())
      {
        _consumerRate.tick();
        DbusEventInternalWritable e = itr.next();
        
        if ( ! e.isValid())
        {
          throw new RuntimeException("Event (" + e + ") is invalid");
        }
        
        //System.out.println("Event in Client Consumer:" + e.toString());
        _maxScnConsumed = Math.max(_maxScnConsumed, e.sequence());
        e.writeTo(_writeChannel,Encoding.JSON);
        itr.remove();
      }
    } finally {
      if (null != itr)
        _eventBuffer.releaseIterator(itr);
    }
  }
  
  @Override
  public List<Long> getErrorOccurences()
  {
    // TODO Auto-generated method stub
    return new ArrayList<Long>();
  }

  @Override
  public HashSet<Integer> getSourceIds()
  {
    // TODO Auto-generated method stub
    return _srcIds;
  }

  public void setSourceIds(HashSet<Integer> srcIds)
  {
    _srcIds = srcIds;
  }
  @Override
  public long maxScnConsumed()
  {
    // TODO Auto-generated method stub
    return _maxScnConsumed;
  }

  @Override
  public void stopConsumption()
  {
    // TODO Auto-generated method stub
    System.out.println("Stopping RelayConsumer");
    _stopConsumption.set(true);
  }

  @Override
  public double getRate()
  {
    // TODO Auto-generated method stub
    return _consumerRate.getRate();
  }

  @Override
  public void stopConsumptionAndWait()
  {
    // TODO Auto-generated method stub
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

  @Override
  public File getDumpFile()
  {
    // TODO Auto-generated method stub
    return _writeFile;
  }

  @Override
  public int getNumScnNotFoundErrorsSeen()
  {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean isRuntimeError()
  {
    // TODO Auto-generated method stub
    return _consumerRuntimeError;
  }

}
