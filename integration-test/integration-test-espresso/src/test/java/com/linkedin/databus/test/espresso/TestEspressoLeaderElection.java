package com.linkedin.databus.test.espresso;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.linkedin.databus.test.DatabusBaseIntegTest;

public class TestEspressoLeaderElection extends DatabusBaseIntegTest
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
  public void testEspressoClusterManager1()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_cluster_manager_1.test");
  }

  @Test
  public void testEspressoClusterManager2()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_cluster_manager_2.test");
  }

  @Test
  public void testEspressoClusterManager3()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_cluster_manager_3.test");
  }
  @Test
  public void testEspressoClusterManager4()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_cluster_manager_4.test");
  }
  @Test
  public void testEspressoClusterManager5()
    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    runCommandLineTest("espresso_cluster_manager_5.test");
  }

}
