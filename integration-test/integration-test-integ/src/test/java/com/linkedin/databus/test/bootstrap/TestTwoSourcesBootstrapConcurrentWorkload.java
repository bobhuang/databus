package com.linkedin.databus.test.bootstrap;

import java.util.ArrayList;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.databus.client.bootstrap.IntegratedDummyDatabusConsumer;
import com.linkedin.databus.test.DatabusBaseIntegTest;

public class TestTwoSourcesBootstrapConcurrentWorkload extends DatabusBaseIntegTest
{
  private static final long INITIAL_START_SCN = 100;
  private static final long TOTAL_GENERATION_DURATION = 2000 * 10;
  private static final int EVENT_PER_SECOND = 20;
  private static final long TOTAL_WORKLOAD = EVENT_PER_SECOND * TOTAL_GENERATION_DURATION / 1000;
  private static final long BOOTSTRAP_SEEDING_WORKLOAD = TOTAL_WORKLOAD / 10;
  private static final String BOOTSTRAP_PRODUCER_PROPERTY_NAME = "integration-test/config/two-source-bootstrap-producer-config.properties";

  private ArrayList<String> _srcList;
  private ArrayList<Integer> _srcIdList;

  @Override
  @BeforeTest
  public void setUp() throws Exception
  {

	setTestName("TestTwoSourcesBootstrapConcurrentWorkLoad");
    super.setUp();

    // source1 maps to id 1, source1 maps to id 2 - it's hard-coded for now
    _srcList = new ArrayList<String>();
    _srcList.add("source1");
    _srcList.add("source2");
    _srcIdList = new ArrayList<Integer>();
    _srcIdList.add(1);
    _srcIdList.add(2);

  }

  @Override
  @AfterTest
  public void tearDown() throws Exception
  {
    super.tearDown();
  }

  @Test
  public void testTwoSourcesBootstrapConcurrentWorkload()
    throws Exception
  {
    // populate initial workload
    generateNumEvents(BOOTSTRAP_SEEDING_WORKLOAD, INITIAL_START_SCN, EVENT_PER_SECOND, _srcIdList);

    // wait for events to be put into relay
    waitForInputDone(_relayInStatsMBean, BOOTSTRAP_SEEDING_WORKLOAD, TOTAL_GENERATION_DURATION * 5);
    long numEventsPopulated = _relayInStatsMBean.getNumDataEvents();
    Assert.assertEquals(BOOTSTRAP_SEEDING_WORKLOAD,
                 numEventsPopulated, "Unexpected number of events populated in relay");

    // start bootstrap producer to initialize bootstrap db
    // noted that the producer can NOT be started before workload is generated on relay
    // it is a bug that will be fixed later
    // we shall have a test coverage for that once it's fixed - LG
    operateBootstrapProducer(SERVICE_OPERATION_START, BOOTSTRAP_PRODUCER_PROPERTY_NAME);

    // wait for bootstrap db to be populated
    waitForInputDone(_bootstrapProducerInStatsMBean, BOOTSTRAP_SEEDING_WORKLOAD, TOTAL_GENERATION_DURATION * 5);
    long bootstrapEndScn = _bootstrapProducerInStatsMBean.getMaxSeenWinScn();
    resumeWorkloadGen(TOTAL_WORKLOAD);

    _consumer = new IntegratedDummyDatabusConsumer(_databusBaseDir + CLIENT_RESULT_DIR + getTestName() + CONSUMER_EVENT_FILE_NAME);
    _consumer.initConn(_srcList);

    _consumer.start();

    // bootstrapEndScn shall be no less
    waitForConsumerBootstrapScn(bootstrapEndScn, _consumer, TOTAL_GENERATION_DURATION * 5);

    // wait for workload generation to finish so we can get the max windown scn
    long expectedNumEvents = TOTAL_WORKLOAD + BOOTSTRAP_SEEDING_WORKLOAD;
    waitForInputDone(_relayInStatsMBean, expectedNumEvents, TOTAL_GENERATION_DURATION * 5);

    numEventsPopulated = _relayInStatsMBean.getNumDataEvents();
    Assert.assertEquals(expectedNumEvents,
                 numEventsPopulated, "Unexpected number of events populated in relay");

    long maxScn = _relayInStatsMBean.getMaxSeenWinScn();
    waitForConsumerRelayScn(maxScn, _consumer, TOTAL_GENERATION_DURATION * 5);

    // compare the final result (ignore scn field because the are useless in bootstrap case)
    ArrayList<String> ignoredFields = new ArrayList<String>();
    ignoredFields.add("scn");
    checkResult(_databusBaseDir + CLIENT_RESULT_DIR +  getTestName() + CONSUMER_EVENT_FILE_NAME, true, ignoredFields);

    // shutdown all components
    shutdownConsumer();
    operateBootstrapProducer(SERVICE_OPERATION_STOP);
  }

}