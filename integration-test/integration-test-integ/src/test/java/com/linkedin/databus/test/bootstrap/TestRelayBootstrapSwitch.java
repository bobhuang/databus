package com.linkedin.databus.test.bootstrap;


import java.io.File;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.databus.client.bootstrap.IntegratedDummyDatabusConsumer;
import com.linkedin.databus.test.DatabusBaseIntegTest;


@Test(singleThreaded=true)
public class TestRelayBootstrapSwitch extends DatabusBaseIntegTest
{
  public static final String MODULE = TestRelayBootstrapSwitch.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private static final long START_SCN = 300;
  private static final int EVENT_PER_SECOND = 5;
  private static final int DEFAULT_EVENT_SIZE = 1000;

  private static final long GENERATION_DURATION = 1000 * 60;
  private static final long INITIAL_BOOTSTRAP_CONSUMPTION_DURATION = 1000 * 8;
  private static final long OVERRUN_RELAYBUFFER_DURATION = GENERATION_DURATION * 2 / 3;
  private static final long REMAINING_WORKLOAD_DURATION = (GENERATION_DURATION - OVERRUN_RELAYBUFFER_DURATION) / 2;
  // The client buffer size shall be small so when pausing it, the remaining buffer space can be quickly
  // filled up and still allows for the relay buffer to ramp around.

  private static final long CLIENT_BUFFER_SIZE =
    EVENT_PER_SECOND * (INITIAL_BOOTSTRAP_CONSUMPTION_DURATION/1000) * DEFAULT_EVENT_SIZE;
  private static final int  READ_BUFFER_SIZE = (int)CLIENT_BUFFER_SIZE / 2;

  private ArrayList<String> _srcList;
  private ArrayList<Integer> _srcIdList;

  @Override
  @BeforeMethod
  public void setUp() throws Exception
  {

    try
    {
      setRelayConfigFile(SMALL_BUFFER_RELAY_PROPERTY_NAME);
      setTestName("RelayBootstrapSwitch");
      super.setUp();
      //Logger.getRootLogger().setLevel(Level.DEBUG);

    }
    catch (Exception e)
    {
      LOG.error("setUp error: " + e.getMessage(), e);
      throw e;
    }

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
  public void testRelayBootstrapSwitch()
    throws Exception
  {
    long initalBootstrapWorkload = EVENT_PER_SECOND * INITIAL_BOOTSTRAP_CONSUMPTION_DURATION / 1000;
    long overrunWorkload = EVENT_PER_SECOND * OVERRUN_RELAYBUFFER_DURATION / 1000;
    long remainingWorkload = EVENT_PER_SECOND * REMAINING_WORKLOAD_DURATION / 1000;

    // start initial workload on relay
    LOG.info("start initial workload on relay");
    generateNumEvents(initalBootstrapWorkload, START_SCN, EVENT_PER_SECOND, _srcIdList);
    long numEventsExpected = initalBootstrapWorkload;


    // wait for the initial bootstrap workload to be put into relay so we can start bootstrap
    LOG.info("wait for the initial bootstrap workload to be put into relay so we can start bootstrap");
    waitForInputDone(_relayInStatsMBean, numEventsExpected, INITIAL_BOOTSTRAP_CONSUMPTION_DURATION * 5);
    long numEventsPopulated = _relayInStatsMBean.getNumDataEvents();
    Assert.assertEquals(numEventsExpected, numEventsPopulated, "Unexpected number of events populated in relay");


    // start bootstrap producer to initialize bootstrap db
    // noted that the producer can NOT be started before workload is generated on relay
    // it is a bug that will be fixed later
    // we shall have a test coverage for that once it's fixed - LG
    LOG.info("start bootstrap producer to initialize bootstrap db");
    operateBootstrapProducer(SERVICE_OPERATION_START);

    // wait for bootstrap db to be populated with the total workload
    LOG.info("wait for bootstrap db to be populated with the total workload");
    waitForInputDone(_bootstrapProducerInStatsMBean,
                     numEventsExpected,
                     INITIAL_BOOTSTRAP_CONSUMPTION_DURATION * 10);
    numEventsPopulated = _bootstrapProducerInStatsMBean.getNumDataEvents();
    long bootstrapEndScn = _bootstrapProducerInStatsMBean.getMaxSeenWinScn();
    Assert.assertEquals(numEventsExpected, numEventsPopulated, "Unexpected number of events populated in bootstrap server");

    LOG.info("Create consumer");
    File testLogDir = new File(_integrationVarLogDir, getTestName());
    File consumerEventFile = new File(testLogDir, CONSUMER_EVENT_FILE_NAME);
    _consumer = new IntegratedDummyDatabusConsumer(consumerEventFile.getAbsolutePath(),
                                                  CLIENT_BUFFER_SIZE,
                                                  READ_BUFFER_SIZE,
                                                  false);

    _consumer.initConn(_srcList);
    _consumer.start();

    // bootstrapEndScn shall be no less
    LOG.info("bootstrapEndScn shall be no less than " + bootstrapEndScn);
    waitForConsumerBootstrapScn(bootstrapEndScn, _consumer, INITIAL_BOOTSTRAP_CONSUMPTION_DURATION * 10);


    // resume for sometime so client can get some events from relay
    LOG.info("resume for sometime so client can get some events from relay");
    resumeWorkloadGen(initalBootstrapWorkload, true);
    numEventsExpected = numEventsExpected + initalBootstrapWorkload;
    waitForInputDone(_relayInStatsMBean,
                     numEventsExpected,
                     INITIAL_BOOTSTRAP_CONSUMPTION_DURATION * 5);
    numEventsPopulated = _relayInStatsMBean.getNumDataEvents();
    Assert.assertEquals(numEventsExpected, numEventsPopulated, "Unexpected number of events populated in relay");

    LOG.info("wait for events");
    waitForInputDone(_bootstrapProducerInStatsMBean,
                     numEventsExpected,
                     INITIAL_BOOTSTRAP_CONSUMPTION_DURATION * 5);
    numEventsPopulated = _bootstrapProducerInStatsMBean.getNumDataEvents();
    Assert.assertEquals(numEventsExpected, numEventsPopulated, "Unexpected number of events populated in bootstrap server");

    // make sure client gets all the events
    LOG.info("make sure client gets all the events");
    long relayMaxScn = _relayInStatsMBean.getMaxSeenWinScn();
    LOG.info("waiting for relayMaxScn:" + relayMaxScn);
    waitForConsumerRelayScn(relayMaxScn, _consumer, INITIAL_BOOTSTRAP_CONSUMPTION_DURATION * 5);

    // pause the client and over run the relay buffer so the min scn is greater than relayMaxScn that client has
    LOG.info("pause the client and over run the relay buffer so the min scn is greater than relayMaxScn that client has");
    _consumer.pause();

    // overrunWorkload is 200, we do 1/4 in each loop so bootstrap won't fall off the relay
    LOG.info("overrunWorkload is 200, we do 1/4 in each loop so bootstrap won't fall off the relay");
    long workloadBatchSize = overrunWorkload / 4;
    for (int i=0; i<4; i++)
    {
      resumeWorkloadGen(workloadBatchSize, true);
      numEventsExpected = numEventsExpected + workloadBatchSize;
      waitForInputDone(_relayInStatsMBean, workloadBatchSize, OVERRUN_RELAYBUFFER_DURATION * 5);
      numEventsPopulated = _relayInStatsMBean.getNumDataEvents();
      Assert.assertEquals(numEventsExpected, numEventsPopulated, "Unexpected number of events populated in relay");

      waitForInputDone(_bootstrapProducerInStatsMBean,
                       numEventsExpected,
                       INITIAL_BOOTSTRAP_CONSUMPTION_DURATION * 5);
      numEventsPopulated = _bootstrapProducerInStatsMBean.getNumDataEvents();
      Assert.assertEquals(numEventsExpected, numEventsPopulated, "Unexpected number of events populated in bootstrap server");
    }

    LOG.info("check if the bootstrap server got all events from relay");
    long overrunRelayMaxScn = _relayInStatsMBean.getMaxSeenWinScn();
    bootstrapEndScn = _bootstrapProducerInStatsMBean.getMaxSeenWinScn();
    LOG.info("checking for overrunRelayMaxScn == bootstrapEndScn == " + overrunRelayMaxScn);
    Assert.assertEquals(overrunRelayMaxScn, bootstrapEndScn, "bootstrap server didn't get all events from relay");

    // resume client and make sure it goes to bootstrap server again
    LOG.info("resume client and make sure it goes to bootstrap server again:" + bootstrapEndScn);
    _consumer.resume();
    waitForConsumerBootstrapScn(bootstrapEndScn, _consumer, OVERRUN_RELAYBUFFER_DURATION * 20);

    // resume workload and let all of them to be finished by the client
    LOG.info("resume workload and let all of them to be finished by the client");
    resumeWorkloadGen(remainingWorkload);
    numEventsExpected = numEventsExpected + remainingWorkload;
    LOG.info("numEventsExpected=" + numEventsExpected);
    waitForInputDone(_relayInStatsMBean, numEventsExpected, GENERATION_DURATION * 5);
    numEventsPopulated = _relayInStatsMBean.getNumDataEvents();
    Assert.assertEquals( numEventsExpected, numEventsPopulated, "Unexpected number of events populated in relay");

    // make sure the consumer gets all events
    LOG.info("make sure the consumer gets all events");
    relayMaxScn = _relayInStatsMBean.getMaxSeenWinScn();
    LOG.info("relayMaxScn=" + relayMaxScn);
    waitForConsumerRelayScn(relayMaxScn, _consumer, GENERATION_DURATION * 5);

    // compare the final result (ignore scn field because the are useless in bootstrap case)
    LOG.info("compare the final result (ignore scn field because the are useless in bootstrap case)");
    ArrayList<String> ignoredFields = new ArrayList<String>();
    ignoredFields.add("scn");
    checkResult(consumerEventFile.getAbsolutePath(), true, ignoredFields);

    // shutdown all components
    LOG.info("shutting down all components");
    shutdownConsumer();

    LOG.info("consumer is shutdown");
    operateBootstrapProducer(SERVICE_OPERATION_STOP);
    LOG.info("bootstrap producer is shutdown");
  }
}
