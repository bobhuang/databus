package com.linkedin.databus.test.relay;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.databus.test.DatabusBaseIntegTest;

public class TestRelayIntegrationOne extends DatabusBaseIntegTest
{

  @Override
  @BeforeTest
  public void setUp() throws Exception
  {
    // skip the super Setup. Just load the view root
    setTestName("TestRelayIntegrationOne");
    setupLogger();
    loadSystemProperties();
    LOG.info("Setup Complete: " + getTestName());

  }

  @Override
  @AfterTest
  public void tearDown() throws Exception
  {
    // skip the super Setup. Just load the view root
    LOG.info("Test Complete: " + getTestName());
  }

  @Test
  public void testRelayMultipleRelay1()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("relay_multiple_relay_1.test");
  }

  @Test
  public void testRelayMultipleRelay2()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("relay_multiple_relay_2.test");
  }

  @Test
  public void testRelayMultipleRelay2Timelines()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("relay_multiple_relay_two_timelines.test");
  }

  @Test
  public void testRelayMultipleRelay2TimelinesRepeated()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("relay_multiple_relay_two_timelines_repeated.test");
  }

  @Test
  public void testRelayBizfollowZookeeper1()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("relay_bizfollow_zookeeper_1");
  }


  @Test
  public void testRelayStreamFromLast()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("relay_stream_from_last.test");
  }

  /** this test starts one relay with multiple buffer - one for bizfollow and one for liar
   * then after generating data for both - it will start two consumers and verify that
   * consumed and generated data match */
  @Test
  public void testMultipleBufferRelay()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("mult_buf_bizfollow_liar.test");
  }


  @Test
  public void testRelayRangeFilter()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("relay_liar_range_filter.test");
  }

  @Test
  public void testRelayModFilter()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("relay_liar_mod_filter.test");
  }

  @Test
  public void testRelayFaultCloseOnRequest()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("./relay_fault_close_on_request.test");
  }
  
  @Test
  public void testRelayScnChunking()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("relay_liar_scn_chunking.test");
  }
  
  @Test
  public void testRelayTxnChunking()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("relay_liar_txn_chunking.test");
  } 
}

