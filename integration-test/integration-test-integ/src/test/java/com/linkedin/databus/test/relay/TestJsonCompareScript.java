package com.linkedin.databus.test.relay;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.linkedin.databus.test.DatabusBaseIntegTest;

public class TestJsonCompareScript extends DatabusBaseIntegTest
{

  @Before
  public void setUp() throws Exception
  {
    // skip the super Setup. Just load the view root
	setTestName("TestJsonCompareScript");
    setupLogger();	
    loadSystemProperties();
    LOG.info("Setup Complete: " + getTestName());

  }

  @After
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
