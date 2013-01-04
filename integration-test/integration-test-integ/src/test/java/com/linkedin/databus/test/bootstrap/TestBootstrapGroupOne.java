package com.linkedin.databus.test.bootstrap;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Level;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.databus.client.netty.NettyHttpDatabusBootstrapConnection;
import com.linkedin.databus.test.DatabusBaseIntegTest;

@Test(singleThreaded=true)
public class TestBootstrapGroupOne extends DatabusBaseIntegTest
{
  public static final String MODULE = TestBootstrapGroupOne.class.getName();

  @Override
  @BeforeMethod
  public void setUp() throws Exception
  {
    // skip the super Setup. Just load the view root
	setTestName("TestBootstrapGroupOne");
    setupLogger();
    NettyHttpDatabusBootstrapConnection.LOG.setLevel(Level.DEBUG);
    loadSystemProperties();
    LOG.info("Setup Complete: " + getTestName());
  }

  @Override
  @AfterMethod
  public void tearDown() throws Exception
  {
    // skip the super Setup. Just load the view root
    LOG.info("Test Complete: " + getTestName());

  }

  @Test
  public void testBootstrapBizfollowGenerator1()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("bootstrap_bizfollow_generator_1.test");
  }

  @Test
  public void testBootstrapBizfollowGenerator2()
  throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("bootstrap_bizfollow_generator_2.test");
  }

  @Test
  public void testBootstrapBizfollowGenerator3()
  throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    // test the log roll over during bootstrap catch up
    runCommandLineTest("bootstrap_bizfollow_generator_3.test");
  }

  @Test
  public void testBootstrapBizfollowDb1()
  throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("bootstrap_bizfollow_db_1.test");
  }

  @Test
  public void testBootstrapLiarDb1()
  throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("bootstrap_liar_db_1.test");
  }

  @Test
  public void testBootstrapMultipleRelay1()
  throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("bootstrap_multiple_relay_1.test");
  }

  @Test
  public void testBootstrapMultipleRelays2Timelines()
  throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("bootstrap_full_multiple_relays_two_timelines.test");
  }

  @Test
  public void testBootstrapProducerFailover2Timelines()
  throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("bootstrap_producer_failover_two_timelines.test");
  }

  @Test
  public void testBootstrapProducerStartupValidation()
  throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("./bootstrap_bizfollow_producer_ckpt_validation.test");
  }
}
