package com.linkedin.databus.test.bootstrap;


import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.databus.client.bootstrap.IntegratedDummyDatabusConsumer;
import com.linkedin.databus.test.DatabusBaseIntegTest;

@Test(singleThreaded=true)
public class TestTwoSourcesBootstrap extends DatabusBaseIntegTest
{
  public static final Logger LOG = Logger.getLogger(TestTwoSourcesBootstrap.class);

  private static final long INITIAL_START_SCN = 100;
  private static final long INITIAL_GENERATION_DURATION = 1000 * 2;
  private static final int EVENT_PER_SECOND = 20;
  private static final String BOOTSTRAP_PRODUCER_PROPERTY_NAME = "integration-test/config/two-source-bootstrap-producer-config.properties";

  private ArrayList<String> _srcList;
  private ArrayList<Integer> _srcIdList;

  @Override
  @BeforeMethod
  public void setUp() throws Exception
  {
    setTestName("TestTwoSourcesBootstrap");
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
  @AfterMethod
  public void tearDown() throws Exception
  {
    super.tearDown();
  }

  @Test
  public void testTwoSourcesStaticBootstrap()
    throws Exception
  {
    // populate initial workload
    long totalWork = EVENT_PER_SECOND * INITIAL_GENERATION_DURATION / 1000;
    LOG.info(String.format("******** generating %d events ******", totalWork));
    generateNumEvents(totalWork, INITIAL_START_SCN, EVENT_PER_SECOND, _srcIdList);

    // wait for events to be put into relay
    LOG.info(String.format("******** waiting for %d events ******", totalWork));
    waitForInputDone(_relayInStatsMBean, totalWork, INITIAL_GENERATION_DURATION * 5);
    long numEventsPopulated = _relayInStatsMBean.getNumDataEvents();
    Assert.assertEquals( totalWork, numEventsPopulated, "Unexpected number of events populated in relay");

    // start bootstrap producer to initialize bootstrap db
    // noted that the producer can NOT be started before workload is generated on relay
    // it is a bug that will be fixed later
    // we shall have a test coverage for that once it's fixed - LG
    LOG.info("******** starting bootstrap producer ******");
    operateBootstrapProducer(SERVICE_OPERATION_START, BOOTSTRAP_PRODUCER_PROPERTY_NAME);

    // wait for bootstrap db to be populated
    LOG.info(String.format("******** waiting for bootstrap producer to get %d events ******", totalWork));
    waitForInputDone(_bootstrapProducerInStatsMBean, totalWork, INITIAL_GENERATION_DURATION * 5);

    LOG.info("******** creating consumer ******");
    _consumer = new IntegratedDummyDatabusConsumer(_databusBaseDir + CLIENT_RESULT_DIR + getTestName() + CONSUMER_EVENT_FILE_NAME);
    _consumer.initConn(_srcList);

    LOG.info("******** starting consumer ******");
    _consumer.start();

    // wait for client to consumer all events
    LOG.info(String.format("******** waiting for consumer to get %d events ******", totalWork));
    waitForOutputDone(_bootstrapServerOutStatsMBean, totalWork, INITIAL_GENERATION_DURATION * 2);

    // compare the final result (ignore scn field because the are useless in bootstrap case)
    ArrayList<String> ignoredFields = new ArrayList<String>();
    ignoredFields.add("scn");
    LOG.info("Checking result");
    checkResult(_databusBaseDir + CLIENT_RESULT_DIR + getTestName() + CONSUMER_EVENT_FILE_NAME, true, ignoredFields);

    // shutdown all components
    LOG.info("shutting down consumer");
    shutdownConsumer();
    LOG.info("shutting down bootstrap producer");
    operateBootstrapProducer(SERVICE_OPERATION_STOP);
  }
}
