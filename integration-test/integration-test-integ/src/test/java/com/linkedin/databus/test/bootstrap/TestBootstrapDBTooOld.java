package com.linkedin.databus.test.bootstrap;


import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.linkedin.databus.client.bootstrap.IntegratedDummyDatabusConsumer;
import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.test.DatabusBaseIntegTest;

public class TestBootstrapDBTooOld extends DatabusBaseIntegTest
{
  public static final String MODULE = TestBootstrapDBTooOld.class.getName();

  private static final long START_SCN = 300;
  private static final int EVENT_PER_SECOND = 5;
  private static final long GENERATION_DURATION = 1000 * 60;
  private static final long INITIAL_BOOTSTRAP_CONSUMPTION_DURATION = 1000 * 8;
  private static final long OVERRUN_RELAYBUFFER_DURATION = GENERATION_DURATION * 2 / 3;
  private ArrayList<String> _srcList;
  private ArrayList<Integer> _srcIdList;

  @Override
  @Before
  public void setUp() throws Exception
  {
    setRelayConfigFile(SMALL_BUFFER_RELAY_PROPERTY_NAME);
    setTestName("TestBootstrapDBTooOld");
    
    super.setUp();

    // source1 maps to id 1 - it's hard-coded for now
    _srcList = new ArrayList<String>();
    _srcList.add("source1");
    _srcIdList = new ArrayList<Integer>();
    _srcIdList.add(1);
  }

  @Override
  @After
  public void tearDown() throws Exception
  {
    super.tearDown();
  }

  @Test
  public void testBootstrapProducerFell()
    throws Exception
  {
    long initalBootstrapWorkload = EVENT_PER_SECOND * INITIAL_BOOTSTRAP_CONSUMPTION_DURATION / 1000;
    long overrunWorkload = EVENT_PER_SECOND * OVERRUN_RELAYBUFFER_DURATION / 1000;

    // start initial workload on relay
    LOG.info("start initial workload on relay");
    generateNumEvents(initalBootstrapWorkload, START_SCN, EVENT_PER_SECOND, _srcIdList);
    long numEventsExpected = initalBootstrapWorkload;

    // wait for the initial bootstrap workload to be put into relay so we can start bootstrap
    LOG.info("wait for the initial bootstrap workload to be put into relay so we can start bootstrap");
    waitForInputDone(_relayInStatsMBean, numEventsExpected, INITIAL_BOOTSTRAP_CONSUMPTION_DURATION * 5);
    long numEventsPopulated = _relayInStatsMBean.getNumDataEvents();
    assertEquals("Unexpected number of events populated in relay", numEventsExpected, numEventsPopulated);

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
                     INITIAL_BOOTSTRAP_CONSUMPTION_DURATION * 5);
    numEventsPopulated = _bootstrapProducerInStatsMBean.getNumDataEvents();
    assertEquals("Unexpected number of events populated in bootstrap server", numEventsExpected, numEventsPopulated);

    _bootstrapServerOutHttpStatsMBean.reset();

//Thread.sleep(100000000);
    LOG.info("Pausing bootstrap producer");
    _bootstrapProducerAdminMBean.pause();
    long numEventsBeforePause = numEventsPopulated;

    assertEquals("Bootstrap Producer is in unexpectd state!",
                 DatabusComponentStatus.Status.PAUSED,
                 DatabusComponentStatus.Status.valueOf(_bootstrapProducerAdminMBean.getStatus()));

    // overrun relay buffer
    LOG.info("overrun relay buffer");
    resumeWorkloadGen(overrunWorkload, true);
    numEventsExpected = numEventsExpected + overrunWorkload;
    waitForInputDone(_relayInStatsMBean, numEventsExpected, OVERRUN_RELAYBUFFER_DURATION * 5);
    numEventsPopulated = _relayInStatsMBean.getNumDataEvents();
    assertEquals("Unexpected number of events populated in relay", numEventsExpected, numEventsPopulated);

    numEventsPopulated = _bootstrapProducerInStatsMBean.getNumDataEvents();
    assertEquals("Bootstrap Producer received additional events after paused",
                 numEventsBeforePause,
                 numEventsPopulated);

    LOG.info("Resuming bootstrap producer");
    _bootstrapProducerAdminMBean.resume();
    LOG.info("Bootstrap producer resumed");

    // make sure the producer returns suspended_on_error status because it fail off the relay
    waitForStatus(_bootstrapProducerAdminMBean,
                  DatabusComponentStatus.Status.SUSPENDED_ON_ERROR,
                  OVERRUN_RELAYBUFFER_DURATION);

    // make sure the bootstrap server returns the correct status
    // Note that bootstrap server status will change to error status only when it starts processing client request..
    _consumer = new IntegratedDummyDatabusConsumer(_databusBaseDir + CLIENT_RESULT_DIR + getTestName() + CONSUMER_EVENT_FILE_NAME);
    _consumer.initConn(_srcList);
    _consumer.start();

    waitForStatus(_bootstrapServerAdminMBean,
                  DatabusComponentStatus.Status.RUNNING,
                  OVERRUN_RELAYBUFFER_DURATION * 5);
    LOG.info("Sleeping !!");
    Thread.sleep(10*1000);
    LOG.info("The number of TooOld Errors :" + _bootstrapServerOutHttpStatsMBean.getNumErrReqDatabaseTooOld());
    Assert.assertTrue("Unexpected Num Fell Off Errors",0 <_bootstrapServerOutHttpStatsMBean.getNumErrReqDatabaseTooOld());
  }
}