package com.linkedin.databus.tests.inprocess;

import java.nio.channels.Pipe;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.Encoding;
import com.linkedin.databus.core.InvalidEventException;
import com.linkedin.databus.core.util.RateMonitor;

public class DatabusPipeProducer
  extends Thread
{
  private final DbusEventBuffer _eventBuffer;
  private final Pipe            _pipe;
  private final AtomicBoolean  _stopPipe = new AtomicBoolean(false);
  private final CountDownLatch _pipeStopped;
  private final RateMonitor    _pipeRate;  
  private boolean               _invalidEventError;
  
  public DatabusPipeProducer(DbusEventBuffer eventBuffer, Pipe pipe)
  {
    _eventBuffer = eventBuffer;
    _pipe = pipe;
    _pipeRate = new RateMonitor("PipeProducer");
    _pipeStopped = new CountDownLatch(1);
    setName("DatabusPipeProducer");
  }
  
  public void stopPipeAndWait()
  {
    // TODO Auto-generated method stub
	_stopPipe.set(true);
    while ( true)
    {
      try
      {
    	_pipeStopped.await();
        break;
      } catch ( InterruptedException ie) { 
      }
    }
  }
  
  @Override
  public void run()
  {
	_pipeRate.start();
    try
    {
      /*
      byte[] b = new byte[1024*1024*10];
      ByteBuffer buf = ByteBuffer.wrap(b);
      DbusEvent event = new DbusEvent();
      while ( !_stopPipe.get())
      {
        int numBytes = _pipe.source().read(buf);
        event.reset(buf, 0);
        System.out.println(Thread.currentThread().getName() + "- Read " +  numBytes + " bytes.");
        System.out.println("Event: " + event);
        System.out.println("Valid Event :" + event.isValid());
        buf.clear();
      }
      */
      while ( ! _stopPipe.get() )
      {
    	 int numEvents = _eventBuffer.readEvents(_pipe.source(),Encoding.BINARY);
    	 //System.out.println("Out of readEvents");
    	 _pipeRate.ticks(numEvents);
      }
    } catch ( InvalidEventException ie) {
      ie.printStackTrace();
      _invalidEventError = true;
    } catch (Exception ex) {
      ex.printStackTrace();
    } finally {
    	_pipeRate.stop();
    	_pipeStopped.countDown();
    }
  }
  
  public boolean isInvalidEventSeen()
  {
    return _invalidEventError;
  }
  
  public double getRate()
  {
	  return _pipeRate.getRate();
  }
  
}
