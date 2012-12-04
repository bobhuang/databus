package com.linkedin.databus.test.espresso;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.test.DatabusBaseIntegTest;

public class TestEspressoDynamicClient2 extends DatabusBaseIntegTest
{
  @Override
  @BeforeClass(groups = { "integration", "espresso", "espressoClient" })
  public void setUp() throws Exception
  {
    // skip the super Setup. Just load the view root
    setTestName("TestEspressoDynamicClient2");
    setupLogger();
    loadSystemProperties();
    LOG.info("Setup Complete: " + getTestName());
  }
  @Override
  @AfterClass(groups = { "integration", "espresso", "espressoClient" })
  public void tearDown() throws Exception
  {
    // skip the super Setup. Just load the view root
  }
  @Test(groups = { "integration", "espresso", "espressoClient" })
  public void testEspressoDynamicClient8()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_client_5_3_2_test8.test");
  }
  @Test(groups = { "integration", "espresso", "espressoClient" })
  public void testEspressoDynamicClient9()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_client_5_3_2_test9.test");
  }
  @Test(groups = { "integration", "espresso", "espressoClient" })
  public void testEspressoDynamicClient10()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_client_5_3_2_test10.test");
  }
  @Test(groups = { "integration", "espresso", "espressoClient" })
  public void testEspressoDynamicClient11()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_client_5_3_2_test11.test");
  }
  @Test(groups = { "integration", "espresso", "espressoClient" })
  public void testEspressoDynamicClient12()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_client_5_3_2_test12.test");
  }
  @Test(groups = { "integration", "espresso", "espressoClient" })
  public void testEspressoDynamicClient13()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_client_5_3_2_test13.test");
  }
  @Test(groups = { "integration", "espresso", "espressoClient" })
  public void testEspressoDynamicClient14()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_client_5_3_2_test14.test");
  }
}
