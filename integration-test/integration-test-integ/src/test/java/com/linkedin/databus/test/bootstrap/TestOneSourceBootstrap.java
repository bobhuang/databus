package com.linkedin.databus.test.bootstrap;

import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.databus.client.bootstrap.IntegratedDummyDatabusConsumer;
import com.linkedin.databus.test.DatabusBaseIntegTest;

public class TestOneSourceBootstrap extends DatabusBaseIntegTest
{
  public static final String MODULE = TestOneSourceBootstrap.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private static final long INITIAL_START_SCN = 100;
  private static final long INITIAL_GENERATION_DURATION = 1000;
  private static final int EVENT_PER_SECOND = 20;

  private ArrayList<String> _srcList;
  private ArrayList<Integer> _srcIdList;


  @Override
  @BeforeTest
  public void setUp() throws Exception
  {
    setTestName("TestOneSourceBootstrap");

    super.setUp();

    // source1 maps to id 1 - it's hard-coded for now
    _srcList = new ArrayList<String>();
    _srcList.add("source1");
    _srcIdList = new ArrayList<Integer>();
    _srcIdList.add(1);
    LOG.info("setUp complete" + getTestName());

  }

  @Override
  @AfterTest
  public void tearDown() throws Exception
  {
    super.tearDown();

  }

  @Test
  public void testStaticBootstrap()
    throws Exception
  {
    LOG.info("Generating events ...");

    // populate initial workload
    long totalWork = EVENT_PER_SECOND * INITIAL_GENERATION_DURATION / 1000;
    generateNumEvents(totalWork, INITIAL_START_SCN, EVENT_PER_SECOND, _srcIdList);

    LOG.info("waiting for input done ...");

    // wait for events to be put into relay
    waitForInputDone(_relayInStatsMBean, totalWork, INITIAL_GENERATION_DURATION * 5);
    long numEventsPopulated = _relayInStatsMBean.getNumDataEvents();
    Assert.assertEquals(totalWork, numEventsPopulated, "Unexpected number of events populated in relay");

    LOG.info("Starting bootstrap producer ...");

    // start bootstrap producer to initialize bootstrap db
    // noted that the producer can NOT be started before workload is generated on relay
    // it is a bug that will be fixed later
    // we shall have a test coverage for that once it's fixed - LG
    operateBootstrapProducer(SERVICE_OPERATION_START);

    LOG.info("Waiting for input done ...");

    // wait for bootstrap db to be populated
    waitForInputDone(_bootstrapProducerInStatsMBean, totalWork, INITIAL_GENERATION_DURATION * 5);

    LOG.info("Starting consumer ...");

    _consumer = new IntegratedDummyDatabusConsumer(_databusBaseDir + CLIENT_RESULT_DIR + getTestName() + CONSUMER_EVENT_FILE_NAME);

    _consumer.initConn(_srcList);

    _consumer.start();

    LOG.info("Waiting for output done");

    // wait for client to consumer all events
    waitForOutputDone(_bootstrapServerOutStatsMBean, totalWork, INITIAL_GENERATION_DURATION * 5);

    //TODO fix me with backoff wait
    Thread.sleep(1000);

    // compare the final result (ignore scn field because the are useless in bootstrap case)
    ArrayList<String> ignoredFields = new ArrayList<String>();
    ignoredFields.add("scn");

    LOG.info("Checking results");

    checkResult(_databusBaseDir + CLIENT_RESULT_DIR + getTestName() + CONSUMER_EVENT_FILE_NAME, true, ignoredFields);

    LOG.info("Shutting down consumer");

    // shutdown all components
    shutdownConsumer();

    LOG.info("Stopping bootstrap producer");

    operateBootstrapProducer(SERVICE_OPERATION_STOP);
  }
}
