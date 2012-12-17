package com.linkedin.databus.test.relay;



import java.io.IOException;
import java.util.concurrent.TimeoutException;

import javax.management.MalformedObjectNameException;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


import com.linkedin.databus.core.monitoring.mbean.DbusEventsTotalStatsMBean;
import com.linkedin.databus.test.DatabusBaseIntegTest;
import com.linkedin.databus.test.ExternalCommand;
import com.linkedin.databus.test.JMXAgentHelper;


public class TestDbRelayJmxStats extends DatabusBaseIntegTest
{
  public static final String DB_RELAY_SERVICE_COMPONENT = "db_relay";
  public static final String OPERATION_START = "start";
  public static final String OPERATION_STOP = "stop";
  public static final String DB_RELAY_PROPERTY_NAME = "integration-test/config/relay-config-small-1.properties";
  public static final String DB_CONFIG_FILE_NAME = "config/sources-bizfollow.json";
  public static final String DB_RELAY_INBOUND_STATS_MBEAN_NAME =
    "com.linkedin.databus2:ownerId=1234,dimension=eventsInbound.total,type=AggregatedDbusEventsTotalStats";
  private String _testName;

  @Override
@BeforeTest
  public void setUp() throws Exception
  {
    _testName = "testDbRelayJmxStats";
    setupLogger();
    loadSystemProperties();
    reinit();
    operateDBRelay(OPERATION_START);
    createDBRelayInboundStatsBean();
  }

  @Override
@AfterTest
  public void tearDown() throws Exception
  {
    closeJMXAgentHelper(_relayJMXHelper);
    _relayJMXHelper = null;
    operateDBRelay(OPERATION_STOP);

  }

  @Test
  public void testDbRelayJmxStats() throws Exception
  {
    // get the initial relay jmx values
    long initialMaxWindowScn = _relayInStatsMBean.getMaxSeenWinScn();
    long initialTotalDataEvents = _relayInStatsMBean.getNumDataEvents();

    // start workload generation
    dbWorkloadGen();

    // wait for relay jmx stats to be populated
    waitForInputDone(_relayInStatsMBean, initialTotalDataEvents + 1, 5000);
    long totalDataEvents = _relayInStatsMBean.getNumDataEvents();
    Assert.assertTrue(totalDataEvents > initialTotalDataEvents,
               "relay inbound total data events not populated!");
    long maxWindowScn = _relayInStatsMBean.getMaxSeenWinScn();
    Assert.assertTrue(maxWindowScn > initialMaxWindowScn,
               "relay inbound max. window scn not populated!");
  }

  private void dbWorkloadGen() throws IOException, InterruptedException, TimeoutException
  {
    ExternalCommand cmd = ExternalCommand.executeWithTimeout(_scriptDir,
                                                             WORKLOAD_GEN_SCRIPT,
                                                             EXEC_TIMEOUT,
                                                             "--db_gen",
                                                             SOURCES_LIST_OPTION_STR, "bizfollow",
                                                             FROM_SCN_OPTION_STR, "2",
                                                             "--db_testdata_reload",
                                                             DB_CONFIG_FILE_OPTION_STR, DB_CONFIG_FILE_NAME);
    processCommandResult(cmd, WORKLOAD_GEN_SCRIPT);
  }

  private void createDBRelayInboundStatsBean() throws IOException, MalformedObjectNameException
  {
    _relayJMXHelper = JMXAgentHelper.create(JMXAgentHelper.makeJMXAgentURLString("localhost", 1099, 9999));
    _relayInStatsMBean = _relayJMXHelper.getMBeanProxy(DB_RELAY_INBOUND_STATS_MBEAN_NAME,
                                                       DbusEventsTotalStatsMBean.class);
  }

  private void operateDBRelay(String operation) throws IOException, InterruptedException, TimeoutException
  {
    ExternalCommand cmd = null;

    cmd = ExternalCommand.executeWithTimeout(_scriptDir,
                                             DRIVER_SCRIPT_NAME,
                                             EXEC_TIMEOUT,
                                             TESTNAME_OPTION_STR,
                                             _testName,
                                             COMPONENT_OPTION_STR,
                                             DB_RELAY_SERVICE_COMPONENT,
                                             SERVICE_OPERATION_OPTION_STR,
                                             operation,
                                             RELAY_PROPERTY_OPTION_STR,
                                             DB_RELAY_PROPERTY_NAME,
                                             DB_CONFIG_FILE_OPTION_STR,
                                             DB_CONFIG_FILE_NAME);
    processCommandResult(cmd, DB_RELAY_SERVICE_COMPONENT + " " + operation);


  }

}
