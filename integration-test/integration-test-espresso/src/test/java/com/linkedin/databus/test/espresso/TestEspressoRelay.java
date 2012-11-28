package com.linkedin.databus.test.espresso;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.linkedin.databus.test.DatabusBaseIntegTest;

public class TestEspressoRelay extends DatabusBaseIntegTest
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
  public void testEspressoRelaySmallBuffer1()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_small_buffer_1.test");
  }

  @Test
  public void testEspressoRelaySmallBuffer2()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_small_buffer_2.test");
  }

  @Test
  public void testEspressoRelaySmallBuffer3()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_small_buffer_3.test");
  }
  @Test
  public void testEspressoRelaySmallBuffer4()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_small_buffer_4.test");
  }
  @Test
  public void testEspressoRelaySmallBuffer5()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_small_buffer_5.test");
  }
  @Test
  public void testEspressoRelaySmallBuffer6()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_small_buffer_6.test");
  }

}
