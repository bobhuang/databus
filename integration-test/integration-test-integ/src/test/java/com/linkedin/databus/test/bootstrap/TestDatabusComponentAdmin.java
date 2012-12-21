package com.linkedin.databus.test.bootstrap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.test.DatabusBaseIntegTest;
import com.linkedin.databus2.core.container.monitoring.mbean.DatabusComponentAdminMBean;

public class TestDatabusComponentAdmin extends DatabusBaseIntegTest
{
  @Override
  @Before
  public void setUp() throws Exception
  {
    super.setUp();
    operateBootstrapProducer(SERVICE_OPERATION_START);
    setTestName("TestDatabusComponentAdmin");
    LOG.info("setup complete" + getTestName());
    
  }

  @Override
  @After
  public void tearDown() throws Exception
  {
    operateBootstrapProducer(SERVICE_OPERATION_STOP);
    super.tearDown();
  }

  @Test
  public void testBasicAdminAttributes() throws Exception
  {
    // relay and bootstrap servers shall be up by now. Only thing needed is the bootstrap producer
    checkBasicAttributes(_relayAdminMBean,
                         "HttpRelay",
                         1234321,
                         9000);
    checkBasicAttributes(_bootstrapServerAdminMBean,
                         "BootstrapHttpServer",
                         1234567,
                         6060);
    checkBasicAttributes(_bootstrapProducerAdminMBean,
                         "DatabusHttpClientImpl",
                         67677877,
                         6767);
  }

  private void checkBasicAttributes(DatabusComponentAdminMBean adminMBean,
                               String componentName,
                               long containerId,
                               int httpPort)
  {
    assertEquals("Component Name doesn't match!", componentName, adminMBean.getComponentName());
    assertEquals("Container Id doesn't match!", containerId, adminMBean.getContainerId());
    assertEquals("Http Port doesn't match!", httpPort, adminMBean.getHttpPort());
    DatabusComponentStatus status =
        new DatabusComponentStatus("test",
                                   DatabusComponentStatus.Status.valueOf(adminMBean.getStatus()),
                                   adminMBean.getStatusMessage());
    assertTrue("Status is not in 'running' status!", status.isRunningStatus());

  }
}
