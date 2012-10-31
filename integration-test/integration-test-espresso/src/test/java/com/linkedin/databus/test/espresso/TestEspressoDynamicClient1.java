package com.linkedin.databus.test.espresso;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.test.DatabusBaseIntegTest;

public class TestEspressoDynamicClient1 extends DatabusBaseIntegTest
{
  @Override
  @BeforeClass(groups = { "integration", "espresso", "espressoClient" })
  public void setUp() throws Exception
  {
    // skip the super Setup. Just load the view root
    setTestName("TestEspressoDynamicClient1");
    loadSystemProperties();
    setupLogger();
  }
  @Override
  @AfterClass(groups = { "integration", "espresso", "espressoClient" })
  public void tearDown() throws Exception
  {
    // skip the super Setup. Just load the view root
  }
  @Test(groups = { "integration", "espresso", "espressoClient" })
  public void testEspressoDynamicClient1()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_client_5_3_2_test1.test");
  }
  @Test(groups = { "integration", "espresso", "espressoClient" })
  public void testEspressoDynamicClient2()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_client_5_3_2_test2.test");
  }
  @Test(groups = { "integration", "espresso", "espressoClient" })
  public void testEspressoDynamicClient3()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_client_5_3_2_test3.test");
  }
  @Test(groups = { "integration", "espresso", "espressoClient" })
  public void testEspressoDynamicClient4()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_client_5_3_2_test4.test");
  }
  @Test(groups = { "integration", "espresso", "espressoClient" })
  public void testEspressoDynamicClient5()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_client_5_3_2_test5.test");
  }

  @Test(groups = { "integration", "espresso", "espressoClient" })
  public void testEspressoDynamicClient6()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_client_5_3_2_test6.test");
  }
  @Test(groups = { "integration", "espresso", "espressoClient" })
  public void testEspressoDynamicClient7()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_client_5_3_2_test7.test");
  }
}
