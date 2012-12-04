package com.linkedin.databus.test.espresso;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.test.DatabusBaseIntegTest;

public class TestEspressoDynamicRelay4 extends DatabusBaseIntegTest
{
  @Override
  @BeforeClass(groups = { "integration", "espresso", "espressoRplDbus" })
  public void setUp() throws Exception
  {
    setTestName(TestEspressoDynamicRelay4.class.getSimpleName());
    // skip the super Setup. Just load the view root
    loadSystemProperties();
    setupLogger();
  }
  @Override
  @AfterClass(groups = { "integration", "espresso", "espressoRplDbus" })
  public void tearDown() throws Exception
  {
    // skip the super Setup. Just load the view root
  }
  @Test(groups = { "integration", "espresso", "espressoRplDbus" })
  public void testEspressoDynamicRPLMGR1()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_dbusmgr_5_3_3_test1.test");
  }
  @Test(groups = { "integration", "espresso", "espressoRplDbus" })
  public void testEspressoDynamicRPLMGR2()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_dbusmgr_5_3_3_test2.test");
  }
  @Test(groups = { "integration", "espresso", "espressoRplDbus" })
  public void testEspressoDynamicRPLMGR3()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_dbusmgr_5_3_3_test3.test");
  }
  @Test(groups = { "integration", "espresso", "espressoRplDbus" })
  public void testEspressoDynamicRPLMGR4()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_dbusmgr_5_3_3_test4.test");
  }
  @Test(groups = { "integration", "espresso", "espressoRplDbus" })
  public void testEspressoDynamicRPLMGR5()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_dbusmgr_5_3_3_test5.test");
  }
  @Test(groups = { "integration", "espresso", "espressoRplDbus" })
  public void testEspressoDynamicRPLMGR6()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_dbusmgr_5_3_3_test6.test");
  }

  @Test(groups = { "integration", "espresso", "espressoRplDbus" })
  public void testEspressoDynamicMultiThreadStats()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_relay_multithread_stats.test");
  }
}
