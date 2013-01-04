package com.linkedin.databus.tests.inprocess;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.container.netty.HttpRelay.Config;
import com.linkedin.databus.container.netty.HttpRelay.StaticConfig;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBuffer.QueuePolicy;
import com.linkedin.databus.core.DbusEventBufferMult;
import com.linkedin.databus.core.Encoding;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.DatabusEventRandomProducer;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.RngUtils;
import com.linkedin.databus2.core.filter.DbusFilter;
import com.linkedin.databus2.core.filter.SourceDbusFilter;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;


public class TestRandomProducerConsumer
{
  private static final int TEN_MB_IN_BYTES =     10000000;
  private static final long ONE_GIGABYTE_IN_BYTES = 1000000000;
  private static final long FIFTEEN_GIGABYTE_IN_BYTES = 15 * ONE_GIGABYTE_IN_BYTES;
  public static final String MODULE = TestRandomProducerConsumer.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private Map<Long,Thread> _consumerThreadMap;
  private Map<Long,DbusInprocessConsumer> _consumerMap;
  private DbusEventBufferMult     _dbuf;
  private DbusEventBuffer _singleEventBuffer;
  private DatabusEventRandomProducer _producer;
  private FileWriterDbusEventListener _producerListener;
  private DbusFilter  _producerFilter;

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
   */
  public static void main(String[] args) throws Exception
  {
    TestRandomProducerConsumer tester = new TestRandomProducerConsumer();
    tester.testProducerConsumer(args);
  }

  public TestRandomProducerConsumer()
  {
    _consumerThreadMap = new HashMap<Long,Thread>();
    _consumerMap = new HashMap<Long, DbusInprocessConsumer>();
  }

  @Test
  public void testProducerConsumer()
  {
    String arg = "-p";
    String arg2 = "integration-test/config/relay-config-small-2.properties";
    String[] args = new String[2];
    args[0] = arg;
    args[1] = arg2;
    try
    {
      testProducerConsumer(args);
    } catch(Exception e) {
      e.printStackTrace();
    }
  }

  public void stopGeneration()
  {
    _producer.stopGeneration();
  }

  public void suspendGeneration()
  {
    _producer.suspendGeneration();
  }

  public boolean isProducerRunning()
  {
    return _producer.checkRunning();
  }

  public boolean isConsumerDone(Long id)
  throws Exception
  {
    if (_consumerMap.containsKey(id))
    {
      DbusInprocessConsumer consumer =  _consumerMap.get(id);
      long maxScnProduced = -1;
      Iterator<Integer> itr = consumer.getSourceIds().iterator();

      while (itr.hasNext())
      {
        long scn = _producerListener.getMaxScnSeen(itr.next());
        maxScnProduced = Math.max(maxScnProduced, scn);
      }

      long maxScnConsumed = consumer.maxScnConsumed();

      if (maxScnProduced < maxScnConsumed)
      {
        throw new Exception(
                            "maxScnProduced less than maxScnConsumed. Produced:" +
                            maxScnProduced + ", Consumed:" + maxScnConsumed);
      }
      return (maxScnProduced == maxScnConsumed);
    }
    return false;
  }


  public void startProducer(String configFile)
  throws Exception
  {
    Properties props = new Properties();
    List<IdNamePair> producerSources = new ArrayList<IdNamePair>();

    producerSources.add(new IdNamePair(1L, "source1"));
    producerSources.add(new IdNamePair(2L, "source2"));
    producerSources.add(new IdNamePair(3L, "source3"));

    HashSet<Integer> sources = new HashSet<Integer>();
    sources.add(1);
    sources.add(2);
    sources.add(3);

    try
    {
      if ( null != _producer)
      {
        props.load(new FileInputStream(configFile));
        File producerFile = getTempFile();
        _producerFilter = new SourceDbusFilter(sources);
        _producerListener = new FileWriterDbusEventListener(producerFile,
                                                            Encoding.JSON,
                                                            _producerFilter,1);

     // for testing create a PhysicalConfig object
        PhysicalSourceConfig pConf = new PhysicalSourceConfig(producerSources);
        _dbuf = new DbusEventBufferMult();

        _singleEventBuffer = new DbusEventBuffer(getConfig(FIFTEEN_GIGABYTE_IN_BYTES,
                                    TEN_MB_IN_BYTES,
                                    QueuePolicy.OVERWRITE_ON_WRITE));
        _singleEventBuffer.addInternalListener(_producerListener);

        _dbuf.addBuffer(pConf.build(), _singleEventBuffer);

        long duration = 200000;
        int rate = 1500;

        _producer = new DatabusEventRandomProducer(_dbuf, 10L, rate, duration,
                                                   producerSources,
                                                   (new com.linkedin.databus.core.util.DatabusEventRandomProducer.Config()).build());

        //Start _producer
        _producer.start();
      }
    } catch (Exception ex) {
      System.out.println("Got exception while starting consumer:" + ex.getMessage());
      throw ex;
    }
  }

  public long startConsumer(String configFile, HashSet<Integer> sourceIds)
  throws Exception
  {
    Properties props = new Properties();
    long id = -1;
    try
    {
      props.load(new FileInputStream(configFile));
      File consumerFile = getTempFile();

      long sleepMinTime = 10;
      long sleepMaxTime = 11;
      int batchFetchMinSize = 500000000; //500MB fetch size
      int batchFetchMaxSize = 500000001;

      int numRetries = 10;

      DatabusDelayConsumer consumer  = new DatabusDelayConsumer(_singleEventBuffer,
                                                                sleepMinTime,
                                                                sleepMaxTime,
                                                                batchFetchMinSize,
                                                                batchFetchMaxSize,
                                                                numRetries,
                                                                sourceIds,
                                                                consumerFile,
                                                                Encoding.JSON);
      Thread consumerThread = new Thread(consumer);
      id = consumerThread.getId();
      _consumerThreadMap.put(id, consumerThread);
      _consumerMap.put(id, consumer);

      //Start Thread
    } catch ( Exception ex) {
      System.out.println("Got exception while starting consumer:" + ex.getMessage());
      throw ex;
    }
    return id;
  }

  public void testProducerConsumer(String[] args) throws Exception
  {
    Properties startupProps = HttpRelay.processCommandLineArgs(args);
    Config config = new Config();

    config.getContainer().setIdFromName("Member2RelayServer.localhost");

    ConfigLoader<StaticConfig> staticConfigLoader =
      new ConfigLoader<StaticConfig>("databus.relay.", config);

    HttpRelay.StaticConfig staticConfig = staticConfigLoader.loadConfig(startupProps);

    LOG.info("source = " + staticConfig.getSourceIds());

    File producerAllFile = getTempFile();
    File producer1File = getTempFile();
    File producer2File = getTempFile();
    File producer3File = getTempFile();

    List<File> producerFiles = new ArrayList<File>();
    producerFiles.add(producerAllFile);
    producerFiles.add(producer1File);
    producerFiles.add(producer2File);
    producerFiles.add(producer3File);

    File consumerAllFile = getTempFile();
    File consumer1File = getTempFile();
    File consumer2File = getTempFile();
    File consumer3File = getTempFile();
    List<File> consumerFiles = new ArrayList<File>();
    consumerFiles.add(consumerAllFile);
    consumerFiles.add(consumer1File);
    consumerFiles.add(consumer2File);
    consumerFiles.add(consumer3File);

    System.out.println("2 ProducerAll file is :" + producerAllFile);
    System.out.println("Producer1 file is :" + producer1File);
    System.out.println("Producer2 file is :" + producer2File);
    System.out.println("Producer3 file is :" + producer3File);

    System.out.println("ConsumerAll File is :" + consumerAllFile);
    System.out.println("Consumer1 File is :" + consumer1File);
    System.out.println("Consumer2 File is :" + consumer2File);
    System.out.println("Consumer3 File is :" + consumer3File);

    HashSet<Integer> consumerSourcesAll = new HashSet<Integer>();
    HashSet<Integer> consumerSources1 = new HashSet<Integer>();
    HashSet<Integer> consumerSources2 = new HashSet<Integer>();
    HashSet<Integer> consumerSources3 = new HashSet<Integer>();

    consumerSourcesAll.add(1);
    consumerSources1.add(1);

    consumerSourcesAll.add(2);
    consumerSources2.add(2);

    consumerSourcesAll.add(3);
    consumerSources3.add(3);

    SourceDbusFilter dbusFilterAll = new SourceDbusFilter(consumerSourcesAll);
    SourceDbusFilter dbusFilter1 = new SourceDbusFilter(consumerSources1);
    SourceDbusFilter dbusFilter2 = new SourceDbusFilter(consumerSources2);
    SourceDbusFilter dbusFilter3 = new SourceDbusFilter(consumerSources3);

    FileWriterDbusEventListener listenerAll = new FileWriterDbusEventListener(producerAllFile,Encoding.JSON,dbusFilterAll,1);
    FileWriterDbusEventListener listener1 = new FileWriterDbusEventListener(producer1File,Encoding.JSON,dbusFilter1,1);
    FileWriterDbusEventListener listener2 = new FileWriterDbusEventListener(producer2File,Encoding.JSON,dbusFilter2,1);
    FileWriterDbusEventListener listener3 = new FileWriterDbusEventListener(producer3File,Encoding.JSON,dbusFilter3,1);

    List<FileWriterDbusEventListener> listeners = new ArrayList<FileWriterDbusEventListener>();
    listeners.add(listenerAll);
    listeners.add(listener1);
    listeners.add(listener2);
    listeners.add(listener3);

    List<IdNamePair> producerSources = new ArrayList<IdNamePair>();
    producerSources.add(new IdNamePair(1L, "source1"));
    producerSources.add(new IdNamePair(2L, "source2"));
    producerSources.add(new IdNamePair(3L, "source3"));

    // for testing create a PhysicalConfig object
    PhysicalSourceConfig pConf = new PhysicalSourceConfig(producerSources);
    DbusEventBufferMult dbufMult = new DbusEventBufferMult();

    DbusEventBuffer dbuf = new DbusEventBuffer(getConfig(FIFTEEN_GIGABYTE_IN_BYTES/150, TEN_MB_IN_BYTES, QueuePolicy.OVERWRITE_ON_WRITE));
    dbuf.addInternalListener(listenerAll);
    dbuf.addInternalListener(listener1);
    dbuf.addInternalListener(listener2);
    dbuf.addInternalListener(listener3);

    dbufMult.addBuffer(pConf.build(), dbuf);


    long duration = 200000;
    int rate = 1500;

    DatabusEventRandomProducer producer = new DatabusEventRandomProducer(dbufMult, 10L, rate,
                                                                         duration, producerSources,staticConfig.getRandomProducer());


    long sleepMinTime = 10;
    long sleepMaxTime = 11;
    int batchFetchMinSize = 500000000; //500MB fetch size
    int batchFetchMaxSize = 500000001;

    int numRetries = 10;

    List<DatabusDelayConsumer> consumerList = new ArrayList<DatabusDelayConsumer>();

    DatabusDelayConsumer consumerAll = new DatabusDelayConsumer(dbuf,
                                                                sleepMinTime,
                                                                sleepMaxTime,
                                                                batchFetchMinSize,
                                                                batchFetchMaxSize,
                                                                numRetries,
                                                                dbusFilterAll,
                                                                consumerAllFile,
                                                                Encoding.JSON);


    DatabusDelayConsumer consumer1 = new DatabusDelayConsumer(dbuf,
                                                              sleepMinTime,
                                                              sleepMaxTime,
                                                              batchFetchMinSize,
                                                              batchFetchMaxSize,
                                                              numRetries,
                                                              dbusFilter1,
                                                              consumer1File,
                                                              Encoding.JSON);

    sleepMinTime = 100;
    sleepMaxTime = 101;
    batchFetchMinSize = 500000000; //500MB fetch size
    batchFetchMaxSize = 500000001;
    DatabusDelayConsumer consumer2 = new DatabusDelayConsumer(dbuf,
                                                              sleepMinTime,
                                                              sleepMaxTime,
                                                              batchFetchMinSize,
                                                              batchFetchMaxSize,
                                                              numRetries,
                                                              dbusFilter2,
                                                              consumer2File,
                                                              Encoding.JSON);

    DatabusDelayConsumer consumer3 = new DatabusDelayConsumer(dbuf,
                                                              sleepMinTime,
                                                              sleepMaxTime,
                                                              batchFetchMinSize,
                                                              batchFetchMaxSize,
                                                              numRetries,
                                                              dbusFilter3,
                                                              consumer3File,
                                                              Encoding.JSON);
    consumerList.add(consumerAll);
    consumerList.add(consumer1);
    consumerList.add(consumer2);
    consumerList.add(consumer3);

    Thread producerThread = new Thread(producer);
    producerThread.start();
    System.out.println("Started producer thread, Will wait for it to die");

    List<Thread> consumerThreadList = new ArrayList<Thread>();

    Thread consumerThreadAll = new Thread(consumerAll);
    Thread consumerThread1 = new Thread(consumer1);
    Thread consumerThread2 = new Thread(consumer2);
    Thread consumerThread3 = new Thread(consumer3);

    consumerThreadList.add(consumerThreadAll);
    consumerThreadList.add(consumerThread1);
    consumerThreadList.add(consumerThread2);
    consumerThreadList.add(consumerThread3);

    for (Thread t : consumerThreadList)
    {
      t.start();
    }

    System.out.println("Started consumer threads, Will wait for them to complete/die");

    try
    {
      Thread.sleep(2*1000);
    } catch (InterruptedException ie) {
      System.out.println("Got interruted while sleeping...");
    }

    producer.stopGeneration();
    producerThread.join();

    for (FileWriterDbusEventListener listener : listeners)
    {
      listener.close();
    }

    System.out.println("Producer thread dies.");

    //long maxScnProduced = producer.maxScn();
    boolean consumed = false;
    int numWaitCycles = 30;
    while (!consumed && (numWaitCycles > 0))
    {
      numWaitCycles--;
      try
      {
        Thread.sleep(2*1000);
      } catch (InterruptedException ie) {
        System.out.println("Got interruted while sleeping...");
      }

      consumed = true;
      for(int i = 0; i < consumerList.size(); i++)
      {
        long scnConsumed = consumerList.get(i).maxScnConsumed();
        long scnProduced = listeners.get(i).maxScnSeen();


        System.out.println("ScnProduced: " + scnProduced + ",scnConsumed: " + scnConsumed);
        assertTrue(scnConsumed <= scnProduced);
        if ( scnConsumed !=  scnProduced)
        {
          consumed = false;
          break;
        }
      }
    }

    for (DbusInprocessConsumer consumer : consumerList)
    {
      consumer.stopConsumption();
    }

    for (Thread t : consumerThreadList)
    {
      t.join();
      System.out.println("Consumer Thread dies");
    }

    int c = 0;
    for (DbusInprocessConsumer consumer : consumerList)
    {
      c++;
      compareProducerConsumer("Producer_Consumer" + c + " Test", listeners.get(c-1),consumer);
    }
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


  public void testUtils()
  {
    int max = 30;
    int min = 5;

    while ( true)
    {
      int rnd = RngUtils.randomPositiveInt();
      int val = min + rnd%(max-min);
      System.out.println("2.Rnd: " + rnd + ",Min:" + min + ",Val:" + val);
      assertTrue(val >= min);
    }
  }
}
