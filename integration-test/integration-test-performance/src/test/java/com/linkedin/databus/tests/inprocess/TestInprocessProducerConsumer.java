package com.linkedin.databus.tests.inprocess;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import junit.framework.AssertionFailedError;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.Test;

public class TestInprocessProducerConsumer
{

  /**
   * @param args
   */
	  public static void main(String[] args)
			    throws Exception
			  {
		    	PatternLayout defaultLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c} %m%n");
		    	ConsoleAppender defaultAppender = new ConsoleAppender(defaultLayout);

		    	Logger.getRootLogger().removeAllAppenders();
		    	Logger.getRootLogger().addAppender(defaultAppender);
		    
		  		Logger.getRootLogger().setLevel(Level.ERROR);
		  
			    // TODO Auto-generated method stub
			    TestInprocessProducerConsumer tester = new TestInprocessProducerConsumer();
			    //tester.testError();
			    if (args.length >= 1) {
			    	GnuParser parser = new GnuParser();
			    	Options options = new Options();
			    	options.addOption("a", "policy", true, "Specify allocation policy : DIRECT_MEMORY / MMAPPED_MEMORY");
			    	options.addOption("n", "buffers", true, "Specify number of buffers");
			    	options.addOption("q", "qps", true, "Specify QPS");
			    	options.addOption("v", "variance", true, "Specify Maximum SCN Variance");
			    	options.addOption("d", "duration", true, "Specify duration for the test run in seconds.");
			    	options.addOption("p", "producer-prop", true, "Specify path for producer properties");
			    	options.addOption("c", "consumer-prop", true, "Specify path for consumer properties");
			    	CommandLine cli = parser.parse( options, args );
			    	
			    	String allocationPolicy = "MMAPPED_MEMORY";
			    	int numBuffers = 30;
			    	int qps = 50;
			    	Double variance = 0.5;
			    	int   duration = 5 * 60 * 1000; // 5 mins
			    	String producerFile = "/Users/csoman/linkedin_setup/trunk/integration-test/config/inprocess/producer.properties";
			  	    String consumerFile = "/Users/csoman/linkedin_setup/trunk/integration-test/config/inprocess/consumer.properties";
			    	
			    	if (cli.hasOption("a")) {
			    		allocationPolicy = cli.getOptionValue("a");
			    	}
			    	if (cli.hasOption("n")) {
			    		numBuffers = new Integer(cli.getOptionValue("n"));
			    	}
			    	if (cli.hasOption("q")) {
			    		qps = new Integer(cli.getOptionValue("q"));
			    	}
			    	if (cli.hasOption("v")) {
			    		variance = new Double(cli.getOptionValue("v"));
			    	}
			    	if (cli.hasOption("d")) {
			    		duration = new Integer(cli.getOptionValue("d"));
			    		duration *= 1000;
			    	}
			    	if (cli.hasOption("p")) {
			    		producerFile = cli.getOptionValue("p");
			    	}
			    	if (cli.hasOption("c")) {
			    		consumerFile = cli.getOptionValue("c");
			    	}
			    	
			    	tester.testMultiBuffer(producerFile,consumerFile,allocationPolicy, numBuffers, qps, variance, duration);
			    } else
			    	tester.testOutput();
			  }

  public class ProducerConfigFileFilter
     implements FilenameFilter
  {
    @Override
    public boolean accept(File dir, String name)
    {
      return name.startsWith("inprocess_producer_");
    }
  }

  @Test
  /*
   * Simulates an end to end pipeline
   *
   * Flow:
   * DbusRandomProducer -EventBuffer-> DatabusDelayConsumer -Pipe-> DbusPipeProducer -EventBuffer-> DbusClientConsumer -> File
   * <------------------ relay ---------------------------><---n/w--><---------------- Consumer ------------------------>
   *
   * It tests the following API of EventBuffer
   *
   * 1. startEvent,appendEvent,endEvent
   * 2. StreamEvents
   * 3. readEvents
   * 4. DbusEventIteratorr
   *
   * The expected events are collected by creating a listener on the relay DbusEventBuffer and writing it to a file.
   * This file is then compared with the consumer file for any missed/corrupted events.
   */
  public void testEnd2End()
    throws Exception
  {
    System.out.println("TestOutput begins !!");
    String producerFile = "../config/inprocess/producer.properties";
    String consumerFile = "../config/inprocess/consumer.properties";

    File f = new File(producerFile);

    System.out.println(f.getAbsolutePath());

    Properties producerProps = getProperties(producerFile);
    Properties consumerProps = getProperties(consumerFile);
    int duration = 2 * 60 * 1000;
    producerProps.setProperty("databus.relay.randomProducer.duration",new Integer(duration).toString());
    producerProps.setProperty("databus.relay.randomProducer.minLength",new Integer(10000).toString());
    producerProps.setProperty("databus.relay.randomProducer.maxLength",new Integer(10000 + 1).toString());
    producerProps.setProperty("databus.relay.randomProducer.eventRate",new Integer(300).toString());
    String testName = "EventSize:" + 1000 + "_NumConumers:" + 1 + "_eps:" + 300;
    TestResult result = testInprocess(testName,producerProps, consumerProps,1,true,100000, true);

    System.out.println("End2End Inprocess Test");
    System.out.println("Result is :" + result);

    if ( ! result.isPassed())
    {
      throw new AssertionFailedError("End2End Inprocess Test Failed !!");
    }
  }


  @Test
  /*
   * Thie method tests the relay side EventBuffer use-case for varying Event Sizes, Event Rate and number of consumers
   *
   * It tests the following API of EventBuffer
   *
   * 1. startEvent,appendEvent,endEvent
   * 2. StreamEvents
   * The expected events are collected by creating a listener on the relay DbusEventBuffer and writing it to a file.
   * This file is then compared with the file generated by consumer for any missed/corrupted events.
   *
   */
  
  public void testOutput()
    throws Exception
  {
    System.out.println("TestOutput begins !!");
    String producerFile = "/Users/csoman/linkedin_setup/trunk/integration-test/config/inprocess/producer.properties";
    String consumerFile = "/Users/csoman/linkedin_setup/trunk/integration-test/config/inprocess/consumer.properties";

    File f = new File(producerFile);
    System.out.println(f.getAbsolutePath());

    Properties producerProps = getProperties(producerFile);
    Properties consumerProps = getProperties(consumerFile);

    int[] eventSizes = { 10 }; //, 100, 1000, 1000 }; //, 100, 1000, 10000};
    int[] numConsumers = { 10 }; //, 50 };
    int   duration = 1 * 60 * 1000; //1 min
    int[] qps    = { 300 };

    producerProps.setProperty("databus.relay.randomProducer.duration",new Integer(duration).toString());

    Map<String, TestResult> testResults = new HashMap<String, TestResult>();

    for ( int k = 0; k < eventSizes.length; k++ )
    {
        producerProps.setProperty("databus.relay.randomProducer.minLength",new Integer(eventSizes[k]).toString());
        producerProps.setProperty("databus.relay.randomProducer.maxLength",new Integer(eventSizes[k] + 1).toString());

        for ( int i = 0; i < numConsumers.length; i++)
        {
            for ( int j = 0;  j < qps.length; j++ )
            {
                producerProps.setProperty("databus.relay.randomProducer.eventRate",new Integer(qps[j]).toString());
                String testName = "EventSize:" + eventSizes[k] + "_NumConumers:" + numConsumers[i] + "_eps:" + qps[j];
                TestResult result = testInprocess(testName,producerProps, consumerProps,numConsumers[i],true,10000, false);
                testResults.put(testName, result);
            }
        }
    }

    System.out.println("RelayBuffer Inprocess Test");
    for (TestResult result : testResults.values())
    {
      System.out.println(result);
    }
  }
  
  private void deleteBufferDir (File file) {
	  try {
		FileUtils.deleteDirectory(file);
	} catch (IOException e) {
		e.printStackTrace();
	}
  }
  
  public void testMultiBuffer(String producerFile, String consumerFile, String allocationPolicy, int numBuffers, int qps, Double scnVariance, int duration)
		  throws Exception
   {
	  System.out.println("Test Multi Buffer begins !!");
	  System.out.println("Allocation policy : " + allocationPolicy);
	  System.out.println("# Buffers         : " + numBuffers);
	  System.out.println("QPS               : " + qps);
	  System.out.println("Max SCN Variance  : " + scnVariance);
	  System.out.println("Test Duration (s) : " + duration/1000);
	  System.out.println("Producer prop     : " + producerFile);
	  System.out.println("Consumer prop     : " + consumerFile);
	  
	  File f = new File(producerFile);
	  System.out.println("Allocation policy being used : " + (allocationPolicy == null?"none":allocationPolicy));
	  System.out.println(f.getAbsolutePath());

	  Properties producerProps = getProperties(producerFile);
	  Properties consumerProps = getProperties(consumerFile);

	  /**** Test Config iteration settings ****/
	  int eventSize = 20000;  // ~ 20 K

//	  int[] numBuffers = {40};//{30,40,50};
//	  int   duration = 5 * 60 * 1000; //1 min
//	  int[] qps = {50,60,40};
//	  Double[] scnVariance = {0.5, 0.75, 0.9};
	  
	  int buffs = numBuffers;
	  int rate = qps;
	  Double variance = scnVariance;
	  Long bufSize = (long) (rate*eventSize*3600);

	  Map<String, TestResult> testResults = new HashMap<String, TestResult>();
	  producerProps.setProperty("databus.relay.randomProducer.duration",new Integer(duration).toString());
	  producerProps.setProperty("databus.relay.randomProducer.minLength",new Integer(eventSize).toString());
	  producerProps.setProperty("databus.relay.randomProducer.maxLength",new Integer(eventSize + 1).toString());

	  String testName = "EventSize:" + eventSize + "_NumConsumers:" + buffs + "_eps:" + rate;
	  TestResult result = testMultipleInprocess(testName,producerProps, consumerProps, buffs, rate, bufSize, variance, 10000, allocationPolicy);
	  testResults.put(testName, result);

	  System.out.println("****************** RelayBuffer Inprocess Test => #Buffers : " + buffs + " Rate : " + rate + " Variance : " + variance);
	  for (TestResult res : testResults.values())
	  {
		  System.out.println(res);
	  }
   }  
  

  @Test
  /*
   * This is perf test ( No Pass/Fail ) for relay buffer where producer is run with varying event rate,
   *  event sizes and number of consumers and checks if we see any SCNNotFound errors.
   *
   */
  public void testError()
     throws Exception
  {
    System.out.println("TestError begins !!");
    String producerFile = "../config/inprocess/producer.properties";
    String consumerFile = "../config/inprocess/consumer.properties";

    File f = new File(producerFile);

    System.out.println(f.getAbsolutePath());

    Properties producerProps = getProperties(producerFile);
    Properties consumerProps = getProperties(consumerFile);

    int[] eventSizes = { 10, 100, 1000, 10000 };
  //  int[] eventSizes = {  10240 };

    int[] numConsumers = { 20, 50 };
 //   int[] numConsumers = { 10, 20, 100 };
    //int   duration = 30 * 1000;

    int   duration = 1 * 60 * 1000; //1 min
    int[] qps    = { 100000 };

    producerProps.setProperty("databus.relay.randomProducer.duration",new Integer(duration).toString());

    Map<String, TestResult> testResults = new HashMap<String, TestResult>();

    for ( int k = 0; k < eventSizes.length; k++ )
    {
        producerProps.setProperty("databus.relay.randomProducer.minLength",new Integer(eventSizes[k]).toString());
        producerProps.setProperty("databus.relay.randomProducer.maxLength",new Integer(eventSizes[k] + 1).toString());

        for ( int i = 0; i < numConsumers.length; i++)
        {
        	for ( int j = 0;  j < qps.length; j++ )
        	{
        		producerProps.setProperty("databus.relay.randomProducer.eventRate",new Integer(qps[j]).toString());
        		String testName = "EventSize:" + eventSizes[k] + "_NumConumers:" + numConsumers[i] + "_eps:" + qps[j];
        		TestResult result = testInprocess(testName,producerProps, consumerProps,numConsumers[i],false,0,false);
        		testResults.put(testName, result);
        	}
        }
    }

    System.out.println("Test ScnNotFoundErrors :");
    for (TestResult result : testResults.values())
    {
      System.out.println(result);
    }
  }


  private void test()
    throws Exception
  {
     int[] numConsumers = { 100, 200, 300};

     int eventsPerWindow = 5;

     String d1 = "../config/inprocess";
     String d2 = "integration-test/config/inprocess";

     String dir = d1;
     File dirF = new File(dir);

     if ( ! dirF.exists())
     {
     	dir = d2;
    	dirF = new File(d2);
     }

     String consumerFile = dir + "/" + "inprocess_consumer.properties";

     List<Double> epsList = new ArrayList<Double>();
     File[] producerFiles = dirF.listFiles(new ProducerConfigFileFilter());

     if ( null == producerFiles)
     {
    	 System.out.println("No ProducerConfigFile found to runn the test !!" + dirF.getAbsolutePath());
    	 throw new RuntimeException("No ProducerConfigFile found to runn the test !!" + dirF.getAbsolutePath());
     }

     int c = 0;
     for (int k = 0; k < producerFiles.length; k++)
     {
       c++;
       String producerFile = producerFiles[k].getAbsolutePath();

       System.out.print(producerFile);
       for ( int i = 0 ; i < numConsumers.length; i++ )
       {
         TestResult result = testInprocess(producerFile, producerFile, consumerFile, numConsumers[i],false,0,false);

         System.out.print("\t" + result.getProducerRate());
         epsList.add(result.getProducerRate());
         trySleep(2000);
       }
       System.out.println();
     }

     System.out.print("ProducerConfig");
     for ( int i = 0; i < numConsumers.length; i++)
     {
       System.out.print("\t EPS_For_Consumers_" + numConsumers[i]);
     }
     //System.out.println();

     int numConfig = numConsumers.length;

     int i = 0;
     for ( double eps : epsList)
     {
       if ( i++ % numConfig == 0)
         System.out.print("\n" + producerFiles[i/numConfig].getName());

       System.out.print("\t" + eps);
     }

  }

  public class TestResult
  {
    public String getName()
    {
      return _name;
    }

    public double getProducerRate()
    {
      return _producerQPS;
    }

    public void setProducerRate(double _producerQPS)
    {
      this._producerQPS = _producerQPS;
    }

    public List<Double> getConsumeRates()
    {
      return _consumerQPS;
    }

    public void setConsumerRates(List<Double> _consumerQPS)
    {
      this._consumerQPS = _consumerQPS;
    }

    public List<Integer> getConsumerErrors()
    {
      return _consumerErrors;
    }

    public List<Long> getConsumerErrorOccurences()
    {
    	 return _consumerErrorOccurences;
    }

    public void setConsumerErrorOccurences(List<Long> occurences)
    {
    	_consumerErrorOccurences = occurences;
    }

    public void setConsumerErrors(List<Integer> _consumerErrors)
    {
      this._consumerErrors = _consumerErrors;
    }

    public void setCompareResults(List<Boolean> compareResults)
    {
      _compareResults = compareResults;
    }


    private double        _producerQPS;
    private List<Double>  _consumerQPS;
    private List<Boolean> _consumerRuntimeErrors;
    private List<Integer> _consumerErrors;
    private final String  _name;
    private List<Long>    _consumerErrorOccurences;
    private List<Boolean> _compareResults;
    private boolean       _passed;


    public boolean isPassed()
    {
      return _passed;
    }

    public void setPassed(boolean passed)
    {
      this._passed = passed;
    }

    public TestResult(String name)
    {
      _name = name;
      _passed = true;
    }

    @Override
    public String toString()
    {
      return "TestResult [_name=" + _name + "Status =" + ((_passed == true) ? "PASSED" : "FAILED")
          + ", _producerQPS=" + _producerQPS + ", _consumerQPS="
          + _consumerQPS + ", _compareResults=" + _compareResults + ", _consumerRuntimeErrors=" + _consumerRuntimeErrors + ", _consumerScnNotFoundErrors=" + _consumerErrors
          + ", _consumerScnNotFoundErrorOccurences=" + _consumerErrorOccurences + "]";
    }
  }
  
  
  private TestResult testInprocess( String name,
                                   String producerConfigFile,
                                   String consumerConfigFile,
                                   int numConsumers,
                                   boolean compareOutput,
                                   long waitTimeForCatchup,
                                   boolean end2End)
    throws Exception
  {
    Properties producerProps  = getProperties(producerConfigFile);
    Properties consumerProps = getProperties(consumerConfigFile);
    return testInprocess(name,producerProps, consumerProps, numConsumers, compareOutput, waitTimeForCatchup, end2End );
  }


  private TestResult testInprocess(String name,
		  Properties producerProps,
		  Properties consumerProps,
		  int numConsumers,
		  boolean compareOutput,
		  long waitTimeForCatchup,
		  boolean end2End)
  {
	  TestResult result = new TestResult(name);
	  try
	  {
		  RandomProducerConsumer runner  = new RandomProducerConsumer();

		  // start Producer
		  runner.startProducer(producerProps,compareOutput);
		  List <Long> consumerIds = new ArrayList<Long>();

		  for ( int i = 0; i < numConsumers; i++ )
		  {
			  consumerIds.add(runner.startConsumer(consumerProps, compareOutput ? null : new NullByteChannel(),end2End));
		  }

		  long duration = runner.getProducerDuration();

		  trySleep(duration);

		  // Stop Producer
		  runner.stopGeneration();
		  result.setProducerRate(runner.getProductionRate());

		  System.out.println("Producer done. Stopping Consumers !!|");

		  if ( compareOutput )
			  trySleep(1000); //Give some time for consumers to catchup


		  List <Integer> consumerErrors = new ArrayList<Integer>();
		  List <Double> consumerRates = new ArrayList<Double>();
		  List<Boolean> consumerRuntimeErrors = new ArrayList<Boolean>();
		  List<Boolean> compareResults = new ArrayList<Boolean>();
		  List<Long> consumerErrorOccurences = new ArrayList<Long>();

		  for ( long id : consumerIds)
		  {
			  System.out.println("Stopping Consumer " + id + " !!");
			  if ( runner.isConsumerRuntimeError(id))
			  {
				  consumerRates.add(-1.0);
				  consumerErrors.add(-1);
				  consumerRuntimeErrors.add(Boolean.TRUE);
				  System.out.println("Consumer " + id + " got some runtime error !!");
			  } else {
				  int numErrors = 0;
				  try
				  {
					  numErrors = runner.stopConsumption(id,compareOutput,waitTimeForCatchup);
				  } catch (Exception ex) {
					  ex.printStackTrace();
				  }

				  consumerErrors.add(numErrors);
				  consumerRates.add(runner.getConsumerRate(id));
				  consumerErrorOccurences.addAll(runner.getConsumerScnNotFoundErrorOccurences(id));
				  consumerRuntimeErrors.add(Boolean.FALSE);

				  if ( compareOutput )
				  {
					  try
					  {
						  runner.testConsumer(id);
						  compareResults.add(Boolean.TRUE);
					  } catch ( Exception ex ) {
						  ex.printStackTrace();
						  compareResults.add(Boolean.FALSE);
						  result.setPassed(false);
					  }
				  }
				  System.out.println("Consumer " + id + " stopped !!");
			  }
		  }

		  result.setConsumerErrors(consumerErrors);
		  result.setConsumerRates(consumerRates);
		  result.setConsumerErrorOccurences(consumerErrorOccurences);
		  result.setCompareResults(compareResults);
	  } catch ( Exception io ) {
		  io.printStackTrace();
		  throw new RuntimeException("Error running test !!" + io);
	  }

	  System.out.println("Inprocess Result is:" + result);
	  return result;
  }


  
  
  private TestResult testMultipleInprocess(String name,
                                  Properties producerProps,
                                  Properties consumerProps,
                                  int numBuffers,
                                  int rate,
                                  Long bufSize,
                                  Double variance,
                                  long waitTimeForCatchup,
                                  String allocationPolicy )
  {
    TestResult result = new TestResult(name);
    try
    {
      MultipleProducersConsumers runner  = new MultipleProducersConsumers();
	  List <Long> consumerIds = new ArrayList<Long>();
      int producerIndex=0;

      // Set producer rate
      producerProps.setProperty("databus.relay.randomProducer.eventRate",new Integer(rate).toString());
      
      for (int m=0;m<numBuffers;m++) {
    	  // start Producer
    	  if (allocationPolicy == null)
    		  producerIndex = runner.startProducer(producerProps);
    	  else
    		  producerIndex = runner.startProducer(producerProps, allocationPolicy, bufSize);

    	  // start Consumer
    	  consumerIds.add(runner.startConsumer(consumerProps, variance));
      }

      // TODO: set duration
      long duration = runner.getProducerDuration();

      trySleep(duration);

      // Stop Producer
      for (int i=0;i<producerIndex;i++) {
    	  runner.stopGeneration(i);
    	  result.setProducerRate(runner.getProductionRate(i));
      }
      
      System.out.println("Producer done. Stopping Consumers !!|");

      List <Integer> consumerErrors = new ArrayList<Integer>();
      List <Double> consumerRates = new ArrayList<Double>();
      List<Boolean> consumerRuntimeErrors = new ArrayList<Boolean>();
      List<Boolean> compareResults = new ArrayList<Boolean>();
      List<Long> consumerErrorOccurences = new ArrayList<Long>();

      for ( long id : consumerIds)
      {
        System.out.println("Stopping Consumer " + id + " !!");
        if ( runner.isConsumerRuntimeError(id))
        {
          consumerRates.add(-1.0);
          consumerErrors.add(-1);
          consumerRuntimeErrors.add(Boolean.TRUE);
          System.out.println("Consumer " + id + " got some runtime error !!");
        } else {
          int numErrors = 0;
          try
          {
            numErrors = runner.stopConsumption(id,false,0);
          } catch (Exception ex) {
            ex.printStackTrace();
          }

          consumerErrors.add(numErrors);
          consumerRates.add(runner.getConsumerRate(id));
          consumerErrorOccurences.addAll(runner.getConsumerScnNotFoundErrorOccurences(id));
          consumerRuntimeErrors.add(Boolean.FALSE);
          System.out.println("Consumer " + id + " stopped !!");
        }
      }

      result.setConsumerErrors(consumerErrors);
      result.setConsumerRates(consumerRates);
      result.setConsumerErrorOccurences(consumerErrorOccurences);
      result.setCompareResults(compareResults);
    } catch ( Exception io ) {
        io.printStackTrace();
        throw new RuntimeException("Error running test !!" + io);
    }

    System.out.println("Inprocess Result is:" + result);
    return result;
  }

  public Properties getProperties(String file)
     throws Exception
  {
    Properties props = new Properties();

    props.load(new FileInputStream(file));

    return props;
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
}
