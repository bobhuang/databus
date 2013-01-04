package com.linkedin.databus.tests.perf;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBufferMult;
import com.linkedin.databus.core.DbusEventBuffer.QueuePolicy;
import com.linkedin.databus.core.util.DatabusEventRandomProducer;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.RngUtils;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;


public class RandomProducerConsumerPerformance {

  private static final int TEN_MB_IN_BYTES =     10000000;
  private static final long ONE_GIGABYTE_IN_BYTES = 1000000000;
  private static final long FIFTEEN_GIGABYTE_IN_BYTES = 15 * ONE_GIGABYTE_IN_BYTES;

  private static DbusEventBuffer.StaticConfig getConfig(long maxEventBufferSize, int maxIndexSize,
                                                        QueuePolicy policy)
  throws InvalidConfigException
  {
    DbusEventBuffer.Config config = new DbusEventBuffer.Config();
    config.setMaxSize(maxEventBufferSize);
    config.setScnIndexSize(maxIndexSize);
    config.setQueuePolicy(policy.toString());
    return config.build();
  }

  /**
   * @param args
   * @throws InterruptedException
   * @throws InvalidConfigException
   */
  public static void main(String[] args)
  throws InterruptedException, InvalidConfigException
  {
    DbusEventBuffer dbuf = new DbusEventBuffer(getConfig(FIFTEEN_GIGABYTE_IN_BYTES, TEN_MB_IN_BYTES, QueuePolicy.OVERWRITE_ON_WRITE));
    long duration = 200000;
    int rate = 1500;
    List<IdNamePair> producerSources = new ArrayList<IdNamePair>();
    producerSources.add(new IdNamePair(1L, "source1"));
    producerSources.add(new IdNamePair(2L, "source2"));
    producerSources.add(new IdNamePair(3L, "source3"));
    
 // for testing create a PhysicalConfig object
    PhysicalSourceConfig pConf = new PhysicalSourceConfig(producerSources);
    DbusEventBufferMult dbufMult = new DbusEventBufferMult();
    dbufMult.addBuffer(pConf.build(), dbuf);
    
    DatabusEventRandomProducer producer = new DatabusEventRandomProducer(dbufMult, 10, rate,
                                                                         duration,
                                                                         producerSources);


    long startSleepTime = 100;
    int batchFetchSize = 500000000; //500MB fetch size
    long processingTimePerEvent = 2;
    int batchesBeforeDeath = 600;
    int retryInterval = 1000;
    int maxRetries = 10;
    HashSet<Integer> consumerSources = new HashSet<Integer>();
    consumerSources.add(1);
    consumerSources.add(2);
    consumerSources.add(3);
    DatabusEventFetcher fetcher = new DatabusEventFetcher(dbuf, RngUtils.randomPositiveLong()%startSleepTime, batchFetchSize, retryInterval, maxRetries, consumerSources);
    DatabusEventDispatcher consumer1 = new DatabusEventDispatcher(fetcher.getClientBuffer(), RngUtils.randomPositiveLong()%startSleepTime, batchFetchSize, processingTimePerEvent, batchesBeforeDeath, retryInterval, maxRetries, consumerSources);
    //	DatabusEventRandomConsumer consumer2 = new DatabusEventRandomConsumer(dbuf, TestUtils.randomLong()%startSleepTime, batchFetchSize, processingTimePerEvent, batchesBeforeDeath, retryInterval, maxRetries);
    Thread producerThread = new Thread(producer);
    producerThread.start();
    Thread fetcherThread = new Thread(fetcher);
    fetcherThread.start();
    Thread consumerThread1 = new Thread(consumer1);
    //Thread consumerThread2 = new Thread(consumer1);
    consumerThread1.start();
    //	consumerThread2.start();
    System.out.println("Started consumer thread, waiting for it to die");
    System.out.println("Started producer thread, waiting for it to die");
    producerThread.join();
    System.out.println("Producer thread dies.");
    consumerThread1.join();
    System.out.println("Consumer thread dies.");
    //	consumerThread2.join();
    System.out.println("Consumer thread dies.");
    fetcherThread.join();
    System.out.println("Fetcher thread dies.");


  }

}
