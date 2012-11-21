package com.linkedin.databus.test.espresso;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.test.DatabusBaseIntegTest;

public class TestEspressoCMIntegrationStatic extends DatabusBaseIntegTest
{

  @Override
  @BeforeClass
  public void setUp() throws Exception
  {
    setTestName(TestEspressoCMIntegrationStatic.class.getSimpleName());
    // skip the super Setup. Just load the view root
    loadSystemProperties();
    setupLogger();
  }

  @Override
  @AfterClass
  public void tearDown() throws Exception
  {
    // skip the super Setup. Just load the view root
  }

  @Test
  public void testEspressoClusterManagerClient1()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_cluster_manager_client_1.test");
  }

  @Test
  public void testEspressoClusterManagerClient2()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_cluster_manager_client_2.test");
  }

  @Test
  public void testEspressoClusterManagerClient3()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_cluster_manager_client_3.test");
  }

  @Test
  public void testEspressoClusterManagerClient4()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_cluster_manager_client_4.test");
  }

  @Test
  public void testEspressoClusterManagerClient5()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_cluster_manager_client_5.test");
  }

  @Test
  public void testEspressoClusterManagerClient6()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_cluster_manager_client_6.test");
  }

  @Test
  public void testEspressoClusterManagerClient7()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_cluster_manager_client_7.test");
  }
}
