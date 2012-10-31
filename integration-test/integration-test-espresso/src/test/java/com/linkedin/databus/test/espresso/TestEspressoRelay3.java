package com.linkedin.databus.test.espresso;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.linkedin.databus.test.DatabusBaseIntegTest;

public class TestEspressoRelay3 extends DatabusBaseIntegTest
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
  public void testEspressoRelay17PartitionBuffer()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_17_partitions.test");
  }

  @Test
  public void testEspressoRelay2Schemas()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_2_schemas.test");
  }

  @Test
  public void testEspressoRelayRandomEvent1()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_random_event_1.test");
  }

  @Test
  public void testEspressoRelayRandomEvent2()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_random_event_2.test");
  }
  
}
