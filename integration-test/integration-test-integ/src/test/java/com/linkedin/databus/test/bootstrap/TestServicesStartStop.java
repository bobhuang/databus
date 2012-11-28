package com.linkedin.databus.test.bootstrap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.linkedin.databus.test.DatabusBaseIntegTest;
import com.linkedin.databus.test.ExternalCommand;


public class TestServicesStartStop extends DatabusBaseIntegTest
{
  @Before
  public void setUp() throws Exception 
  {
	setTestName("TestServicesStartStop");	  
    setupLogger();	
    loadSystemProperties();
    LOG.info("Setup Complete" + getTestName());
  }

  @After
  public void tearDown() throws Exception 
  {
	    LOG.info("Test Complete" + getTestName());
  }
  
  @Test
  public void testRelayService() throws IOException, InterruptedException, TimeoutException
  {
    File scriptDir = new File(_databusBaseDir + INTEGRATION_SCRIPT_DIR);
    ExternalCommand cmd = null;
   
    // START
    cmd = ExternalCommand.executeWithTimeout(scriptDir, 
                                             DRIVER_SCRIPT_NAME, 
                                             EXEC_TIMEOUT, 
                                             COMPONENT_OPTION_STR,
                                             RELAY_SERVICE_COMPONENT,
                                             SERVICE_OPERATION_OPTION_STR,
                                             SERVICE_OPERATION_START,
                                             RELAY_PROPERTY_OPTION_STR, 
                                             RELAY_PROPERTY_NAME);
    if (0 != cmd.exitValue())
    {
      fail(cmd.getStringOutput());
    }
    
    // START another one, it shall fail.
    cmd = ExternalCommand.executeWithTimeout(scriptDir, 
                                             DRIVER_SCRIPT_NAME, 
                                             EXEC_TIMEOUT,
                                             COMPONENT_OPTION_STR,
                                             RELAY_SERVICE_COMPONENT,
                                             SERVICE_OPERATION_OPTION_STR,
                                             SERVICE_OPERATION_START,
                                             RELAY_PROPERTY_OPTION_STR, 
                                             RELAY_PROPERTY_NAME);
 //   System.out.println(cmd.getStringOutput());
    assertFalse(RELAY_SERVICE_COMPONENT + " started twice.", (0 == cmd.exitValue()));
    
    // STOP
    cmd = ExternalCommand.executeWithTimeout(scriptDir, 
                                             DRIVER_SCRIPT_NAME, 
                                             EXEC_TIMEOUT, 
                                             COMPONENT_OPTION_STR,
                                             RELAY_SERVICE_COMPONENT,
                                             SERVICE_OPERATION_OPTION_STR,
                                             SERVICE_OPERATION_STOP,
                                             RELAY_PROPERTY_OPTION_STR, 
                                             RELAY_PROPERTY_NAME);
    if (0 != cmd.exitValue())
    {
      fail(cmd.getStringOutput());
    }
  }
  
  @Test
  public void testBootstrapService() throws IOException, InterruptedException, TimeoutException
  {
    File scriptDir = new File(_databusBaseDir + INTEGRATION_SCRIPT_DIR);
    ExternalCommand cmd = null;
    
    // START
    cmd = ExternalCommand.executeWithTimeout(scriptDir, 
                                             DRIVER_SCRIPT_NAME, 
                                             EXEC_TIMEOUT, 
                                             COMPONENT_OPTION_STR,
                                             BOOTSTRAP_SERVICE_COMPONENT,
                                             SERVICE_OPERATION_OPTION_STR,
                                             SERVICE_OPERATION_START,
                                             BOOTSTRAP_PROPERTY_OPTION_STR, 
                                             _bootstrapServiceConfigFile);
    if (0 != cmd.exitValue())
    {
      fail(cmd.getStringOutput());
    }
    
    // START another one, it shall fail.
    cmd = ExternalCommand.executeWithTimeout(scriptDir, 
                                             DRIVER_SCRIPT_NAME, 
                                             EXEC_TIMEOUT,
                                             COMPONENT_OPTION_STR,
                                             BOOTSTRAP_SERVICE_COMPONENT,
                                             SERVICE_OPERATION_OPTION_STR,
                                             SERVICE_OPERATION_START,
                                             RELAY_PROPERTY_OPTION_STR, 
                                             RELAY_PROPERTY_NAME);
    assertFalse(BOOTSTRAP_SERVICE_COMPONENT + " started twice.", (0 == cmd.exitValue()));
    
    // STOP
    cmd = ExternalCommand.executeWithTimeout(scriptDir, 
                                             DRIVER_SCRIPT_NAME, 
                                             EXEC_TIMEOUT,
                                             COMPONENT_OPTION_STR,
                                             BOOTSTRAP_SERVICE_COMPONENT,
                                             SERVICE_OPERATION_OPTION_STR,
                                             SERVICE_OPERATION_STOP,
                                             BOOTSTRAP_PROPERTY_OPTION_STR, 
                                             _bootstrapServiceConfigFile);
    if (0 != cmd.exitValue())
    {
      fail(cmd.getStringOutput());
    }
  }
}
