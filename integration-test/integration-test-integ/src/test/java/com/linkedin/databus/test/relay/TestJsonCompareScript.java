package com.linkedin.databus.test.relay;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.databus.test.DatabusBaseIntegTest;

public class TestJsonCompareScript extends DatabusBaseIntegTest
{

  @Override
  @BeforeTest
  public void setUp() throws Exception
  {
    // skip the super Setup. Just load the view root
	setTestName("TestJsonCompareScript");
    setupLogger();
    loadSystemProperties();
    LOG.info("Setup Complete: " + getTestName());

  }

  @Override
  @AfterTest
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
