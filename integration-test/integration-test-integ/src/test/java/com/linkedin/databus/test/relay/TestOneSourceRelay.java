package com.linkedin.databus.test.relay;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.linkedin.databus.test.DatabusBaseIntegTest;

public class TestOneSourceRelay extends DatabusBaseIntegTest
{

  @Override
  @Before
  public void setUp() throws Exception
  {
    // skip the super Setup. Just load the view root
	setTestName("TestOneSourceRelay");
    setupLogger();		
    loadSystemProperties();
    LOG.info("Setup Complete: " + getTestName());    
  }

  @Override
  @After
  public void tearDown() throws Exception
  {
    // skip the super Setup. Just load the view root
	LOG.info("Test Complete: " + getTestName());
	  
  }

  @Test
  public void testRelaySmallBuffer1()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("relay_small_buffer_1.test");
  }

  @Test
  public void testRelaySmallBuffer2()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("relay_small_buffer_2.test");
  }

  @Test
  public void testRelaySmallBuffer4()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
	// test relay wrap around
    runCommandLineTest("relay_small_buffer_4.test");
  }

  @Test
  public void testRelaySmallBuffer5()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
	// test with two consumers, suspend-resume event geneartion
    runCommandLineTest("relay_small_buffer_5.test");
  }

  @Test
  public void testRelayLargeBuffer1()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
	// test with large buffer size
    runCommandLineTest("relay_large_buffer_1.test");
  }
}
