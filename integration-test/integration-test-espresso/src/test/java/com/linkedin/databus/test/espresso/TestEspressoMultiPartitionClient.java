package com.linkedin.databus.test.espresso;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.test.DatabusBaseIntegTest;


public class TestEspressoMultiPartitionClient extends DatabusBaseIntegTest
{
  @Override
  @BeforeClass(groups = { "integration", "espresso", "espressoMultiPartitionClient" })
  public void setUp() throws Exception
  {
    setTestName(TestEspressoMultiPartitionClient.class.getSimpleName());
    // skip the super Setup. Just load the view root
    loadSystemProperties();
    setupLogger();
  }
  @Override
  @AfterClass(groups = { "integration", "espresso", "espressoMultiPartitionClient" })
  public void tearDown() throws Exception
  {
    // skip the super Setup. Just load the view root
  }
  @Test(groups = { "integration", "espresso", "espressoMultiPartitionClient" })
  public void testEspressoMultiPartitionClient1()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_mpconsumer_client_test1.test");
  }

  @Test(groups = { "integration", "espresso", "espressoMultiPartitionClient" })
  public void testEspressoMultiPartitionClient2()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_mpconsumer_client_test2.test");
  }

  @Test(groups = { "integration", "espresso", "espressoMultiPartitionClient" })
  public void testEspressoMultiPartitionClient3()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_mpconsumer_client_test3.test");
  }

  @Test(groups = { "integration", "espresso", "espressoMultiPartitionClient" })
  public void testEspressoMultiPartitionClient4()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_mpconsumer_client_test4.test");
  }

  @Test(groups = { "integration", "espresso", "espressoMultiPartitionClient" })
  public void testEspressoMultiPartitionClient5()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_mpconsumer_client_test5.test");
  }

  /*
   * Not ready yet
  @Test(groups = { "integration", "espresso", "espressoMultiPartitionClient" })
  public void testEspressoMultiPartitionClient6()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_mpconsumer_client_test6.test");
  }
  */
  @Test(groups = { "integration", "espresso", "espressoMultiPartitionClient" })
  public void testEspressoMultiPartitionClient7()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_mpconsumer_client_test7.test");
  }
  @Test(groups = { "integration", "espresso", "espressoMultiPartitionClient" })
  public void testEspressoMultiPartitionClient8()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_mpconsumer_client_test8.test");
  }

}
