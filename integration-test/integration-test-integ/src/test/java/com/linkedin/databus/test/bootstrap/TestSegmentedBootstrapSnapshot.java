package com.linkedin.databus.test.bootstrap;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.linkedin.databus.client.bootstrap.IntegratedDummyDatabusConsumer;
import com.linkedin.databus.test.DatabusBaseIntegTest;


public class TestSegmentedBootstrapSnapshot extends DatabusBaseIntegTest
{
  private static final long INITIAL_START_SCN = 100;
  private static final long TOTAL_GENERATION_DURATION = 20000;
  private static final int EVENT_PER_SECOND = 30;
  private static final int DEFAULT_EVENT_SIZE = 1000;
  private static final int TOTAL_SEGMENT = 5;

  // The client buffer size shall be small enough so it needs to
  // make 5 trips to finish.  Given the default event size being 1K
  // and we have 5 * 20 events, the buffer size shall be around 20K.
  private static final long CLIENT_BUFFER_SIZE = EVENT_PER_SECOND * (TOTAL_GENERATION_DURATION/1000) * DEFAULT_EVENT_SIZE / TOTAL_SEGMENT;
  private static final int  READ_BUFFER_SIZE = (int)CLIENT_BUFFER_SIZE / 2;
  private ArrayList<String> _srcList;
  private ArrayList<Integer> _srcIdList;

  @Override
  @Before
  public void setUp() throws Exception
  {

    setTestName("SegmentedBootstrapSnapshot");
    super.setUp();
    Logger.getRootLogger().setLevel(Level.DEBUG);

    // source1 maps to id 1 - it's hard-coded for now
    _srcList = new ArrayList<String>();
    _srcList.add("source1");
    _srcIdList = new ArrayList<Integer>();
    _srcIdList.add(1);
  }

  @Test
  public void testSegmentedBootstrapSnapshot()
    throws Exception
  {
    long totalWorkload = TOTAL_GENERATION_DURATION * EVENT_PER_SECOND / 1000;

    LOG.info("populate initial workload");
    generateNumEvents(totalWorkload, INITIAL_START_SCN, EVENT_PER_SECOND, _srcIdList);

    LOG.info("wait for events to be put into relay: " + totalWorkload);
    waitForInputDone(_relayInStatsMBean, totalWorkload, TOTAL_GENERATION_DURATION * 5);
    long numEventsPopulated = _relayInStatsMBean.getNumDataEvents();
    assertEquals("Unexpected number of events populated in relay", totalWorkload, numEventsPopulated);

    // start bootstrap producer to initialize bootstrap db
    // noted that the producer can NOT be started before workload is generated on relay
    // it is a bug that will be fixed later
    // we shall have a test coverage for that once it's fixed - LG
    LOG.info("start bootstrap producer to initialize bootstrap db");
    operateBootstrapProducer(SERVICE_OPERATION_START);

    LOG.info("wait for bootstrap db to be populated: " + totalWorkload);
    waitForInputDone(_bootstrapProducerInStatsMBean, totalWorkload, TOTAL_GENERATION_DURATION * 5);

    LOG.info("start consumer");
    File testVarLogDir = new File(_integrationVarLogDir, getTestName());
    File consumerEventFile = new File(testVarLogDir, CONSUMER_EVENT_FILE_NAME);
    _consumer = new IntegratedDummyDatabusConsumer(consumerEventFile.getAbsolutePath(),
                                                   CLIENT_BUFFER_SIZE,
                                                   READ_BUFFER_SIZE);

    _consumer.initConn(_srcList);
    _consumer.start();

    long bootstrapEndScn = _bootstrapProducerInStatsMBean.getMaxSeenWinScn();
    LOG.info("bootstrapEndScn shall be no less than " + bootstrapEndScn);
    waitForConsumerBootstrapScn(bootstrapEndScn, _consumer, TOTAL_GENERATION_DURATION * 10);

    // waitForScnAtClient(maxScn, consumer, TOTAL_GENERATION_DURATION * 5);
    // TODO: currently, there isn't a way for bootstrap server to return # of streaming calls it received.
    // Once such DbusHttpTotalStats is available at bootstrap server, enable the following to ensure the
    // expected number of stream calls were made to the bootstrap server
    // assertTrue("Incorrect number of around trips to bootstrap server", TOTAL_SEGMENT, consumer.getNumEndBootstrap)
    // compare the final result (ignore scn field because the are useless in bootstrap case)
    LOG.info("checking events at client:");
    ArrayList<String> ignoredFields = new ArrayList<String>();
    ignoredFields.add("scn");
    checkResult(consumerEventFile.getAbsolutePath(), true, ignoredFields);

    LOG.info("shutting down consumer");
    shutdownConsumer();
    LOG.info("shutting bootstrap producer");
    operateBootstrapProducer(SERVICE_OPERATION_STOP);
    LOG.info("bootstrap producer shutdown");
  }
}