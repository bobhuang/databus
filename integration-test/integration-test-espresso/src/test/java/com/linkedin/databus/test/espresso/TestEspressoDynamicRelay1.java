package com.linkedin.databus.test.espresso;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.test.DatabusBaseIntegTest;

public class TestEspressoDynamicRelay1 extends DatabusBaseIntegTest
{
  @Override
  @BeforeClass(groups = { "integration", "espresso", "espressoDynamicRelay" })
  public void setUp() throws Exception
  {
    setTestName(TestEspressoDynamicRelay1.class.getSimpleName());
    // skip the super Setup. Just load the view root
    loadSystemProperties();
    setupLogger();
  }
  @Override
  @AfterClass(groups = { "integration", "espresso", "espressoDynamicRelay" })
  public void tearDown() throws Exception
  {
    // skip the super Setup. Just load the view root
  }
  @Test(groups = { "integration", "espresso", "espressoDynamicRelay" })
  public void testEspressoDynamicRly1()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_relay_5_3_3_test1.test");
  }
  @Test(groups = { "integration", "espresso", "espressoDynamicRelay" })
  public void testEspressoDynamicRly2()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_relay_5_3_3_test2.test");
  }
  @Test(groups = { "integration", "espresso", "espressoDynamicRelay" })
  public void testEspressoDynamicRly3()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_relay_5_3_3_test3.test");
  }
  @Test(groups = { "integration", "espresso", "espressoDynamicRelay" })
  public void testEspressoDynamicRly4()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_relay_5_3_3_test4.test");
  }
  @Test(groups = { "integration", "espresso", "espressoDynamicRelay" })
  public void testEspressoDynamicRly5()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_relay_5_3_3_test5.test");
  }
  @Test(groups = { "integration", "espresso", "espressoDynamicRelay" })
  public void testEspressoDynamicRly6()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_relay_5_3_3_test6.test");
  }
  @Test(groups = { "integration", "espresso", "espressoDynamicRelay" })
  public void testEspressoDynamicRly7()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_relay_5_3_3_test7.test");
  }
  @Test(groups = { "integration", "espresso", "espressoDynamicRelay" })
  public void testEspressoDynamicRly8()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_relay_5_3_3_test8.test");
  }
  @Test(groups = { "integration", "espresso", "espressoDynamicRelay" })
  public void testEspressoDynamicRly9()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_relay_5_3_3_test9.test");
  }
}
