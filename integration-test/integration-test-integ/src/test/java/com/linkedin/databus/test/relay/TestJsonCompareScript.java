package com.linkedin.databus.test.relay;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.databus.test.DatabusBaseIntegTest;

@Test(singleThreaded=true)
public class TestJsonCompareScript extends DatabusBaseIntegTest
{

  @Override
  @BeforeMethod
  public void setUp() throws Exception
  {
    // skip the super Setup. Just load the view root
	setTestName("TestJsonCompareScript");
    setupLogger();
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
  public void testJsonCompareMatch()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("json_compare_match.test");
  }

  @Test
  public void testJsonCompareProducerEventsMissmatch()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("json_compare_producer_events_missmatch.test");
  }


}
