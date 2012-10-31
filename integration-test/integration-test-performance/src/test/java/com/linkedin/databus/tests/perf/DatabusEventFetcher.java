package com.linkedin.databus.tests.perf;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Set;

import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBuffer.AllocationPolicy;
import com.linkedin.databus.core.DbusEventBuffer.QueuePolicy;
import com.linkedin.databus.core.Encoding;
import com.linkedin.databus.core.InternalDatabusEventsListener;
import com.linkedin.databus.core.InvalidEventException;
import com.linkedin.databus.core.OffsetNotFoundException;
import com.linkedin.databus.core.ScnNotFoundException;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.Utils;
import com.linkedin.databus2.core.filter.DbusFilter;
import com.linkedin.databus2.core.filter.SourceDbusFilter;

public class DatabusEventFetcher implements Runnable {


  private final DbusEventBuffer relayBuffer;
  private final long startSleepTime;
  private final int batchFetchSize;
  private long maxScnConsumed=0;
  private final long retryInterval;
  private final int maxRetries;
  private final DbusFilter dbusFilter;
  private final DbusEventBuffer clientBuffer;


  private DbusEventBuffer.StaticConfig getConfig(long maxEventBufferSize, int maxIndividualBufferSize, int maxIndexSize,
                                                 int maxReadBufferSize, AllocationPolicy allocationPolicy, QueuePolicy policy)
  throws InvalidConfigException
  {
    DbusEventBuffer.Config config = new DbusEventBuffer.Config();
    config.setMaxSize(maxIndividualBufferSize);
    config.setMaxIndividualBufferSize(maxIndividualBufferSize);
    config.setScnIndexSize(maxIndexSize);
    config.setReadBufferSize(maxReadBufferSize);
    config.setAllocationPolicy(allocationPolicy.name());
    config.setQueuePolicy(policy.toString());
    return config.build();
  }

  public DatabusEventFetcher(DbusEventBuffer dbuf, long startSleepTime, int batchFetchSize, long retryInterval, int maxRetries, Set<Integer> sources) throws InvalidConfigException

  {
    this.relayBuffer = dbuf;
    this.startSleepTime = startSleepTime;
    this.batchFetchSize = batchFetchSize;
    this.retryInterval = retryInterval;
    this.maxRetries = maxRetries;
    this.dbusFilter = new SourceDbusFilter(sources);
    this.clientBuffer = new DbusEventBuffer(getConfig(2*batchFetchSize,DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE, 1000,batchFetchSize,AllocationPolicy.DIRECT_MEMORY, QueuePolicy.BLOCK_ON_WRITE));

  }

  public DbusEventBuffer getClientBuffer() {
    return clientBuffer;
  }

  public long maxScnConsumed() {
    return maxScnConsumed;
  }
  @Override
  public void run() {
    try {
      Thread.sleep(startSleepTime);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    int currentBatchId = 0;
    ArrayList<InternalDatabusEventsListener> eventListeners = new ArrayList<InternalDatabusEventsListener>();
    Checkpoint cp = new Checkpoint();
    cp.setFlexible();
    eventListeners.add(cp);
    WritableByteChannel writeChannel = null;
    File directory = new File(".");
    File writeFile;
    ReadableByteChannel readChannel = null;
    try
    {
      writeFile = File.createTempFile("test", ".dbus", directory);
      writeChannel = Utils.openChannel(writeFile, true);
      readChannel = Utils.openChannel(writeFile, false);

    }
    catch (IOException e2)
    {
      // TODO Auto-generated catch block
      e2.printStackTrace();
    }

    int tryNumber = 0;
    int read = 0;
    while (tryNumber < 100) {
      boolean gotEvents = false;
      while (!gotEvents && (tryNumber < 100))
      {
        int currentBatchFetchSize = Math.min(batchFetchSize, clientBuffer.getBufferFreeReadSpace());
        System.out.println("Client buffer free space = " + clientBuffer.getBufferFreeSpace());
        System.out.println("Client buffer free space for read = " + clientBuffer.getBufferFreeReadSpace());
        if (currentBatchFetchSize > 0)
        {

          try {
            System.out.println("Fetcher requesting " + currentBatchFetchSize + " bytes from cp:" + cp);
            relayBuffer.streamEvents(cp, false, currentBatchFetchSize, writeChannel, Encoding.BINARY, dbusFilter);
            read = clientBuffer.readEvents(readChannel, eventListeners);

            gotEvents = true;

            if (read <= 0 ) {
              System.out.println("ReceiveBuffer is empty(), tryNumber = " + tryNumber);

              ++tryNumber;
            }
            else
            {
              System.out.println("Fetcher got some data");
              System.out.println("Checkpoint cp = "+ cp);
              tryNumber = 0;
            }
            cp.checkPoint();
            System.out.println("Fetcher consumed " + currentBatchId + " batches until scn:" + cp);
            try {
              Thread.sleep(retryInterval);
            } catch (InterruptedException e1) {
              e1.printStackTrace();
            }
          }
          catch (InvalidEventException e)
          {
            e.printStackTrace();
            //FIXME Handle exception correctly
          }
          catch (ScnNotFoundException e) {

            e.printStackTrace();
            System.out.println("Waiting... scn not found !!!! : " + cp);
            ++tryNumber;
            try {
              Thread.sleep(retryInterval);
            } catch (InterruptedException e1) {
              e1.printStackTrace();
            }
          }
          catch (OffsetNotFoundException e) {

              e.printStackTrace();
              System.out.println("Waiting... offset not found !!!! : " + cp);
              ++tryNumber;
              try {
                Thread.sleep(retryInterval);
              } catch (InterruptedException e1) {
                e1.printStackTrace();
              }
            }

        }
        ++currentBatchId;
      }
    }
    System.out.println("Fetcher consumed " + currentBatchId + "batches until cp:" + cp +".");
  }

}
