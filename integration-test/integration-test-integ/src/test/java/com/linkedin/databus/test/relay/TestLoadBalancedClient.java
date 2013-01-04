package com.linkedin.databus.test.relay;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


import com.linkedin.databus.test.DatabusBaseIntegTest;

public class TestLoadBalancedClient extends DatabusBaseIntegTest 
{
	  @Override
	  @BeforeMethod
	  public void setUp() throws Exception
	  {
	    // skip the super Setup. Just load the view root
	    setTestName("TestLoadBalancedClient");
	    setupLogger();
	    loadSystemProperties();
	    LOG.info("Setup Complete: " + getTestName());

	  }
	  
	  @Test
	  public void testLoadBalancedSingleClient()
	    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
	  {
	    runCommandLineTest("liar_load_balanced_single_client.test");
	  }

	  @Test
	  public void testLoadBalancedMultipleClients()
	    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
	  {
	    runCommandLineTest("liar_load_balanced_multiple_clients.test");
	  }
	  
	  @Test
	  public void testLoadBalancedMultipleClientsRebalance()
	    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
	  {
	    runCommandLineTest("liar_load_balanced_multiple_clients_rebalance.test");
	  }
	  
	  @Test
	  public void testLoadBalancedMultipleClientsAndClustersRebalance()
	    throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
	  {
	    runCommandLineTest("liar_load_balanced_multiple_clients_clusters_rebalance.test");
	  }
}
