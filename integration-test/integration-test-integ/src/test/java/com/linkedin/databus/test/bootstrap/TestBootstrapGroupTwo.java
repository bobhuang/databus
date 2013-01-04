package com.linkedin.databus.test.bootstrap;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.test.DatabusBaseIntegTest;

@Test(singleThreaded=true)
public class TestBootstrapGroupTwo extends DatabusBaseIntegTest
{
  public static final String MODULE = TestBootstrapGroupTwo.class.getName();

  @Override
  @BeforeClass
  public void setUp() throws Exception
  {
    // skip the super Setup. Just load the view root
	setTestName("TestBootstrapGroupTwo");
    setupLogger();
    loadSystemProperties();
    LOG.info("Setup Complete: " + getTestName());

  }

  @Override
  @AfterClass
  public void tearDown() throws Exception
  {
    // skip the super Setup. Just load the view root
    LOG.info("Test Complete: " + getTestName());

  }


  @Test
  public void testBootstrapLiarFilterEmptyResponse()
      throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("bootstrap_liar_serversidefiltering_emptyResponse.test");
  }


  @Test
  public void testBootstrapLiarRangeFilterCheck()
      throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("bootstrap_liar_serversidefiltering_range_check.test");
  }

  @Test
  public void testBootstrapLiarModFilterCheck()
      throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("bootstrap_liar_serversidefiltering_mod_check.test");
  }



  @Test
  public void testBootstrapLiarRangeCatchupSnapshot()
      throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("bootstrap_liar_serversidefiltering_catchupsnapshot_range_check.test");
  }

  @Test
  public void testBootstrapLiarModCatchupSnapshot()
      throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("bootstrap_liar_serversidefiltering_catchupsnapshot_mod_check.test");
  }


  @Test
  public void testBootstrapLiarRangeWithPredicatePushDown()
      throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("bootstrap_liar_range_filter_predicatePushDown.test");
  }

  @Test
  public void testBootstrapLiarModWithPredicatePushDown()
      throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("bootstrap_liar_mod_filter_predicatePushDown.test");
  }


  @Test
  public void testBootstrapLiarFilterWithRelay()
      throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("bootstrap_liar_range_filter_verifywithrelay.test");
  }

  @Test
  public void testBootstrapBizfollowGeneratorApplier()
      throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("bootstrap_bizfollow_generator_applier.test");
  }

  @Test
  public void testBootstrapBizfollowGenerator4()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("bootstrap_bizfollow_generator_4.test");
  }

  @Test
  public void testBootstrapBizfollowGenerator5()
  throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("bootstrap_bizfollow_generator_5.test");
  }

  @Test
  public void testBootstrapBizfollowGenerator6()
  throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("bootstrap_bizfollow_generator_6.test");
  }

  @Test
  public void testBootstrapBizfollowGenerator7()
  throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("bootstrap_bizfollow_generator_7.test");
  }

  @Test
  public void testBootstrapMember2Generator1()
  throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("bootstrap_member2_generator_1.test");
  }

  @Test
  public void testBootstrapCatchupRestart()
  throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("bootstrap_catchup_restart.test");
  }

  @Test
  public void testBootstrapRangeFilter()
  throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("bootstrap_liar_range_filter.test");
  }

  @Test
  public void testBootstrapModFilter()
  throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("bootstrap_liar_mod_filter.test");
  }

  @Test
  public void testBootstrapBypassSnapshot() throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("bootstrap_bypass_snapshot.test");
  }

  @Test
  public void testBootstrapProducerRestartAfterFellOff() throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("bootstrap_producer_felloff_restart.test");
  }

  @Test
  public void testBootstrapSnapshotRestart1()
  throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("bootstrap_snapshot_restart_1.test");
  }

  @Test
  public void testBootstrapSnapshotRestart2()
  throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("bootstrap_snapshot_restart_2.test");
  }

}
