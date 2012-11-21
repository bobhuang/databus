package com.linkedin.databus.test.espresso;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.test.DatabusBaseIntegTest;

public class TestEspressoDynamicRelay3 extends DatabusBaseIntegTest
{
  @Override
  @BeforeClass(groups = { "integration", "espresso", "espressoDynamicRelay" })
  public void setUp() throws Exception
  {
    setTestName(TestEspressoDynamicRelay3.class.getSimpleName());
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
  public void testEspressoDynamicSchema1()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_schema_5_3_3_test1.test");
  }
  @Test(groups = { "integration", "espresso", "espressoDynamicRelay" })
  public void testEspressoDynamicSchema2()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_schema_5_3_3_test2.test");
  }
  @Test(groups = { "integration", "espresso", "espressoDynamicRelay" })
  public void testEspressoDynamicSchema3()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_schema_5_3_3_test3.test");
  }
  @Test(groups = { "integration", "espresso", "espressoDynamicRelay" })
  public void testEspressoDynamicSchema4()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_schema_5_3_3_test4.test");
  }
  @Test(groups = { "integration", "espresso", "espressoDynamicRelay" })
  public void testEspressoDynamicSchema5()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_schema_5_3_3_test5.test");
  }
}
