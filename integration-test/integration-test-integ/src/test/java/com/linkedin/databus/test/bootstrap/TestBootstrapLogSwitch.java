package com.linkedin.databus.test.bootstrap;

import java.util.ArrayList;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.databus.client.bootstrap.IntegratedDummyDatabusConsumer;
import com.linkedin.databus.test.DatabusBaseIntegTest;


@Test(singleThreaded=true)
public class TestBootstrapLogSwitch extends DatabusBaseIntegTest
{
  private static final long START_SCN = 300;
  private static final int EVENT_PER_SECOND = 10;

  private static final long GENERATION_DURATION = 1000 * 60;

  private ArrayList<String> _srcList;
  private ArrayList<Integer> _srcIdList;

  static
  {
  }

  @Override
  @BeforeMethod
  public void setUp() throws Exception
  {
    setBootstrapServiceConfigFile(SMALL_FETCHSIZE_BOOTSTRAP_PROPERTY_NAME);
    setBootstrapProducerConfigFile(SMALL_LOG_BOOTSTAP_PRODUCER_PROPERTY_NAME);
    setTestName("TestBootstrapLogSwitch");

    super.setUp();
    Logger.getRootLogger().setLevel(Level.INFO);

    // source1 maps to id 1 - it's hard-coded for now
    _srcList = new ArrayList<String>();
    _srcList.add("source1");
    _srcIdList = new ArrayList<Integer>();
    _srcIdList.add(1);
  }

  @Override
  @AfterMethod
  public void tearDown() throws Exception
  {
    super.tearDown();
  }

  @Test
  public void testBootstrapLogSwitch()
    throws Exception
  {
    long bootstrapWorkload = EVENT_PER_SECOND * GENERATION_DURATION / 1000;

    // start initial workload on relay
    generateNumEvents(bootstrapWorkload, START_SCN, EVENT_PER_SECOND, _srcIdList);
    long numEventsExpected = bootstrapWorkload;

    // wait for the initial bootstrap workload to be put into relay so we can start bootstrap
    waitForInputDone(_relayInStatsMBean, numEventsExpected, GENERATION_DURATION * 5);
    long numEventsPopulated = _relayInStatsMBean.getNumDataEvents();
    Assert.assertEquals(numEventsExpected, numEventsPopulated, "Unexpected number of events populated in relay");

    // start bootstrap producer to initialize bootstrap db
    // noted that the producer can NOT be started before workload is generated on relay
    // it is a bug that will be fixed later
    // we shall have a test coverage for that once it's fixed - LG
    operateBootstrapProducer(SERVICE_OPERATION_START);

    // wait for bootstrap db to be populated with the total workload
    waitForInputDone(_bootstrapProducerInStatsMBean,
                     numEventsExpected,
                     GENERATION_DURATION * 10);

    _consumer = new IntegratedDummyDatabusConsumer(_databusBaseDir + CLIENT_RESULT_DIR + getTestName() + CONSUMER_EVENT_FILE_NAME );

    numEventsPopulated = _bootstrapProducerInStatsMBean.getNumDataEvents();
    long bootstrapEndScn = _bootstrapProducerInStatsMBean.getMaxSeenWinScn();
    Assert.assertEquals(numEventsExpected, numEventsPopulated, "Unexpected number of events populated in bootstrap server");

    _consumer.initConn(_srcList);
    _consumer.start();

    // bootstrapEndScn shall be no less
    waitForConsumerBootstrapScn(bootstrapEndScn, _consumer, GENERATION_DURATION * 5);

    // compare the final result (ignore scn field because the are useless in bootstrap case)
    ArrayList<String> ignoredFields = new ArrayList<String>();
    ignoredFields.add("scn");
    checkResult(_databusBaseDir + CLIENT_RESULT_DIR + getTestName() + CONSUMER_EVENT_FILE_NAME, true, ignoredFields);

    // shutdown all components
    shutdownConsumer();
    operateBootstrapProducer(SERVICE_OPERATION_STOP);
  }
}
