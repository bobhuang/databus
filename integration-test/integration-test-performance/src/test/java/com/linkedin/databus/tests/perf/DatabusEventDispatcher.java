package com.linkedin.databus.tests.perf;

import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBuffer.DbusEventIterator;
import com.linkedin.databus2.core.filter.DbusFilter;
import com.linkedin.databus2.core.filter.SourceDbusFilter;
import java.util.Set;

public class DatabusEventDispatcher implements Runnable {

  private final DbusEventBuffer clientBuffer;
  private final long startSleepTime;
  private final int batchFetchSize;
  private final long processingTimePerEvent;
  private final long batchesBeforeDeath;
  private long maxScnConsumed=0;
  private final long retryInterval;
  private final int maxRetries;
  private final DbusFilter dbusFilter;

  public DatabusEventDispatcher(DbusEventBuffer clientBuffer, long startSleepTime, int batchFetchSize, long processingTimePerEvent, long batchesBeforeDeath, long retryInterval, int maxRetries, Set<Integer> sources)
  {
    this.clientBuffer = clientBuffer;
    this.startSleepTime = startSleepTime;
    this.batchFetchSize = batchFetchSize;
    this.processingTimePerEvent = processingTimePerEvent;
    this.batchesBeforeDeath = batchesBeforeDeath;
    this.retryInterval = retryInterval;
    this.maxRetries = maxRetries;
    this.dbusFilter = new SourceDbusFilter(sources);
  }

  public long maxScnConsumed() {
    return maxScnConsumed;
  }
  @Override
  public void run() {
    /*		try {
			Thread.sleep(startSleepTime);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
     */
    int numEvents =0;
    int numWindows = 0;
    int currentBatchId = 0;
    Checkpoint cp = new Checkpoint();
    cp.setFlexible();
    int tryNumber = 0;
    DbusEventIterator eventIterator = clientBuffer.acquireIterator("dispatcher");
    DbusEvent e = null;

    while (tryNumber < 100) {
      boolean gotEvents = false;
      while (!gotEvents && (tryNumber < 10))
      {
        gotEvents = false;
        int i=0;

        while (eventIterator.hasNext())
        {
          gotEvents = true;
          // do stuff

          System.out.println("loop "+ i);
          ++i;
          e = eventIterator.next();
          if (!e.isValid())
          {
            System.out.println("Valid Event = false");
          }
          if (!e.isEndOfPeriodMarker())
          {
            System.out.println("Consume:"+e.sequence());
          }
          else
          {
            System.out.println("Consume:window:"+e.sequence());
          }

          if (cp.getInit()) {
            // fire start Event
            //eventCallback.startEvent(e);
            //cp.startEvent(e);
          }
          // fire on event
          // eventCallback.onEvent(e);
          //	                    System.out.println("Consuming event B:" + e);
          maxScnConsumed = Math.max(maxScnConsumed, e.sequence());
          cp.onEvent(e);
          if (e.isEndOfPeriodMarker())
          {
            ++numWindows;
            eventIterator.remove();
          }
          else
          {
            ++numEvents;
          }

        }


        if (!gotEvents)
        {
          try {
            synchronized(eventIterator)
            {
              while (!eventIterator.hasNext())
              {
                System.out.println("Dispatcher going into wait");
                eventIterator.wait();
                System.out.println("Dispatcher coming out of wait");
              }
            }
          } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
          }
        }

        System.out.println("Consumer consumed " + numEvents + " events in " + numWindows + " windows until scn:" + cp);

      }
    }
    System.out.println("Consumer consumed " + numEvents + "events in " + numWindows + "windows with max scn " + maxScnConsumed +".");
  }

}
