package com.linkedin.databus.test.espresso;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.linkedin.databus.test.DatabusBaseIntegTest;

public class TestEspressoClient2 extends DatabusBaseIntegTest
{

  @Override
  @BeforeClass
  public void setUp() throws Exception
  {
    // skip the super Setup. Just load the view root
    loadSystemProperties();
  }

  @Override
  @AfterClass
  public void tearDown() throws Exception
  {
    // skip the super Setup. Just load the view root
  }

  @Test
  public void testEspressoClientConfig5()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_client_config_5.test");
  }

  @Test
  public void testEspressoClientConfig6()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_client_config_6.test");
  }

  @Test
  public void testEspressoClientConfig7()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_client_config_7.test");
  }

}
