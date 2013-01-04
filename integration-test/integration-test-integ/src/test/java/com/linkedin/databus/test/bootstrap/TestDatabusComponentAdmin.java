package com.linkedin.databus.test.bootstrap;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.test.DatabusBaseIntegTest;
import com.linkedin.databus2.core.container.monitoring.mbean.DatabusComponentAdminMBean;

@Test(singleThreaded=true)
public class TestDatabusComponentAdmin extends DatabusBaseIntegTest
{
  @Override
  @BeforeMethod
  public void setUp() throws Exception
  {
    super.setUp();
    operateBootstrapProducer(SERVICE_OPERATION_START);
    setTestName("TestDatabusComponentAdmin");
    LOG.info("setup complete" + getTestName());

  }

  @Override
  @AfterMethod
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
    Assert.assertEquals(componentName, adminMBean.getComponentName(), "Component Name doesn't match!");
    Assert.assertEquals(containerId, adminMBean.getContainerId(), "Container Id doesn't match!");
    Assert.assertEquals(httpPort, adminMBean.getHttpPort(), "Http Port doesn't match!");
    DatabusComponentStatus status =
        new DatabusComponentStatus("test",
                                   DatabusComponentStatus.Status.valueOf(adminMBean.getStatus()),
                                   adminMBean.getStatusMessage());
    Assert.assertTrue(status.isRunningStatus(), "Status is not in 'running' status!");

  }
}
