package com.linkedin.databus.test.bootstrap;

import java.util.ArrayList;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.databus.client.bootstrap.IntegratedDummyDatabusConsumer;
import com.linkedin.databus.test.DatabusBaseIntegTest;


@Test(singleThreaded=true)
public class TestOneSourceBootstrapConcurrentRelayTraffic extends DatabusBaseIntegTest
{
  private static final long INITIAL_START_SCN = 100;
  private static final long INITIAL_GENERATION_DURATION = 1000 * 80;
  private static final int EVENT_PER_SECOND = 20;
  private static final long INITIAL_BOOTSTRAP_CONSUMPTION_DURATION = INITIAL_GENERATION_DURATION/10;

  private ArrayList<String> _srcList;
  private ArrayList<Integer> _srcIdList;

  @Override
  @BeforeMethod
  public void setUp() throws Exception
  {

    setTestName("BootstrapSwitchConcurrentRelayTraffic");
    super.setUp();

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
  public void testBootstrapWithRelayActivity()
    throws Exception
  {
    // start concurrent workload on relay
    long initalBootstrapWorkload = EVENT_PER_SECOND * INITIAL_BOOTSTRAP_CONSUMPTION_DURATION / 1000;
    long totalWorkload = EVENT_PER_SECOND * INITIAL_GENERATION_DURATION / 1000;
    generateNumEvents(initalBootstrapWorkload, INITIAL_START_SCN, EVENT_PER_SECOND, _srcIdList);

    // wait for the initial bootstrap workload (1/10 of the total workload) to be put into relay so we can start bootstrap
    waitForInputDone(_relayInStatsMBean, initalBootstrapWorkload, INITIAL_BOOTSTRAP_CONSUMPTION_DURATION * 5);
    long numEventsPopulated = _relayInStatsMBean.getNumDataEvents();
    Assert.assertEquals(initalBootstrapWorkload, numEventsPopulated, "Unexpected number of events populated in relay");

    // start bootstrap producer to initialize bootstrap db
    // noted that the producer can NOT be started before workload is generated on relay
    // it is a bug that will be fixed later
    // we shall have a test coverage for that once it's fixed - LG
    operateBootstrapProducer(SERVICE_OPERATION_START);

    // wait for bootstrap db to be populated with 1/10 of the total workload
    waitForInputDone(_bootstrapProducerInStatsMBean, initalBootstrapWorkload, INITIAL_BOOTSTRAP_CONSUMPTION_DURATION * 5);
    long bootstrapEndScn = _bootstrapProducerInStatsMBean.getMaxSeenWinScn();
    resumeWorkloadGen(totalWorkload);

    _consumer = new IntegratedDummyDatabusConsumer(_databusBaseDir + CLIENT_RESULT_DIR + getTestName() + CONSUMER_EVENT_FILE_NAME);

    _consumer.initConn(_srcList);
    _consumer.start();

    // bootstrapEndScn shall be no less
    waitForConsumerBootstrapScn(bootstrapEndScn, _consumer, INITIAL_GENERATION_DURATION * 5);

    // wait for workload generation to finish so we can get the max windown scn
    long expectedNumEvents = totalWorkload + initalBootstrapWorkload;
    waitForInputDone(_relayInStatsMBean, expectedNumEvents, INITIAL_GENERATION_DURATION * 5);
    numEventsPopulated = _relayInStatsMBean.getNumDataEvents();
    Assert.assertEquals( expectedNumEvents, numEventsPopulated, "Unexpected number of events populated in relay");

    long relayMaxScn = _relayInStatsMBean.getMaxSeenWinScn();
    waitForConsumerRelayScn(relayMaxScn, _consumer, INITIAL_GENERATION_DURATION * 5);

    // compare the final result (ignore scn field because the are useless in bootstrap case)
    ArrayList<String> ignoredFields = new ArrayList<String>();
    ignoredFields.add("scn");
    checkResult(_databusBaseDir + CLIENT_RESULT_DIR + getTestName() + CONSUMER_EVENT_FILE_NAME, true, ignoredFields);

    // shutdown all components
    shutdownConsumer();
    operateBootstrapProducer(SERVICE_OPERATION_STOP);
  }
}
