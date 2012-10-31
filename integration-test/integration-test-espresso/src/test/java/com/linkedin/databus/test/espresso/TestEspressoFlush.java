package com.linkedin.databus.test.espresso;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.test.DatabusBaseIntegTest;

@Test(singleThreaded=true)
public class TestEspressoFlush extends DatabusBaseIntegTest
{
  @Override
  @BeforeClass(groups = { "integration", "espresso", "espressoClient" })
  public void setUp() throws Exception
  {
    // skip the super Setup. Just load the view root
    setTestName("TestEspressoFlush");
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
  public void testEspressoFlush1()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_flush_5_3_4_test1.test");
  }
  @Test(groups = { "integration", "espresso", "espressoClient" })
  public void testEspressoFlush2()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_flush_5_3_4_test2.test");
  }
  @Test(groups = { "integration", "espresso", "espressoClient" })
  public void testEspressoFlush3()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_flush_5_3_4_test3.test");
  }
  @Test(groups = { "integration", "espresso", "espressoClient" })
  public void testEspressoFlush4()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_flush_5_3_4_test4.test");
  }
  @Test(groups = { "integration", "espresso", "espressoClient" })
  public void testEspressoFlush5()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_flush_5_3_4_test5.test");
  }
}
