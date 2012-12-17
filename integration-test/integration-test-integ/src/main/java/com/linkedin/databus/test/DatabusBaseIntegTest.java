package com.linkedin.databus.test;



import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import javax.management.MalformedObjectNameException;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;
import org.testng.Assert;

import com.linkedin.databus.bootstrap.monitoring.server.mbean.DbusBootstrapHttpStatsMBean;
import com.linkedin.databus.client.bootstrap.IntegratedDummyDatabusConsumer;
import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.FileBasedEventTrackingCallback;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsTotalStatsMBean;
import com.linkedin.databus2.core.container.monitoring.mbean.DatabusComponentAdminMBean;

public class DatabusBaseIntegTest {
	public final static String MODULE = DatabusBaseIntegTest.class.getName();
	public final static Logger LOG = Logger.getLogger(MODULE);

	// 2 minutes execution timeout
	public static final long EXEC_TIMEOUT = 120/* 000 */;
	public static final String DATABUS_BASED_DIR_TOKEN = "databus.basedir.path";
	public static final String DEFAULT_DATABUS_BASED_DIR = "./";

	public static final String DRIVER_SCRIPT_NAME = "dbus2_driver.py";
	public static final String TESTNAME_OPTION_STR = "-n";
	public static final String DEFAULT_TESTNAME = "default";
	public static final String COMPONENT_OPTION_STR = "-c";
	public static final String RELAY_SERVICE_COMPONENT = "test_relay";
	public static final String BOOTSTRAP_SERVICE_COMPONENT = "bootstrap_server";
	public static final String BOOTSTRAP_PRODUCER_COMPONENT = "test_bootstrap_producer";
	public static final String REINIT_COMPONENT = "bootstrap_dbreset";

	public static final String SERVICE_OPERATION_OPTION_STR = "-o";
	public static final String SERVICE_OPERATION_START = "start";
	public static final String SERVICE_OPERATION_STOP = "stop";

	public static final String RELAY_PROPERTY_OPTION_STR = "-p";
	public static final String RELAY_PROPERTY_NAME = "integration-test/config/test_integ_relay.properties";
	public static final String SMALL_BUFFER_RELAY_PROPERTY_NAME = "integration-test/config/small_integ_relay.properties";

	public static final String BOOTSTRAP_PROPERTY_OPTION_STR = "-p";
	public static final String BOOTSTRAP_PROPERTY_NAME = "config/bootstrap-server-config.properties";
	public static final String SMALL_FETCHSIZE_BOOTSTRAP_PROPERTY_NAME = "integration-test/config/small-fetchsize-bootstrap-service-config.properties";

	public static final String BOOTSTRAP_PRODUCER_OPTION_STR = "-p";
	public static final String BOOTSTRAP_PRODUCER_PROPERTY_NAME = "config/bootstrap-producer-config.properties";
	public static final String SMALL_LOG_BOOTSTAP_PRODUCER_PROPERTY_NAME = "integration-test/config/small-log-producer-config.properties";

	public static final String WORKLOAD_GEN_SCRIPT = "dbus2_gen_event.py";
	public static final String FROM_SCN_OPTION_STR = "--from_scn";
	public static final String EVENT_FREQUENCY_OPTION_STR = "--event_per_sec";
	public static final String DURATION_OPTION_STR = "--duration";
	public static final String NUMBER_OF_EVENTS_OPTION_STR = "--num_events";
	public static final String SOURCES_LIST_OPTION_STR = "--src_ids";
	public static final String PAUSE_WORKLOAD_OPTION_STR = "--suspend_gen";
	public static final String RESUME_WORKLOAD_OPTION_STR = "--resume_gen";
	public static final String WAIT_TIL_SUSPEND_OPTION_STR = "--wait_until_suspend";
	public static final String DB_CONFIG_FILE_OPTION_STR = "--db_config_file";
    public static final String CMD_LINE_OPTION_STRING    = "--cmdline_props";
	public static final String RESULT_VERIFICATION_SCRIPT = "dbus2_json_compare.py";
	// public static final String SOURCE_FILENAME_OPTION_STR = "-s";
	// public static final String DESTINATION_FILENAME_OPTIONS_STR = "-d";

	public static final String CLIENT_RESULT_DIR = "integration-test/var/log/";

	public static final String TESTCASE_OPTION_STR = "--testcase";
    public static final String CONSUMER_EVENT_FILE_NAME = "/consumer_1";

	protected String _databusBaseDir;

	protected FileBasedEventTrackingCallback _trackingCallback;
	protected DbusEventBufferAppendable _relayEventBuffer;

	protected JMXAgentHelper _relayJMXHelper;
	protected JMXAgentHelper _bootstrapServerJMXHelper;
	protected JMXAgentHelper _bootstrapProducerJMXHelper;

	public static final String RELAY_OUTBOUND_STATS_MBEAN_NAME = "com.linkedin.databus2:ownerId=1234321,dimension=eventsOutbound.total,type=AggregatedDbusEventsTotalStats";
	public static final String RELAY_INBOUND_STATS_MBEAN_NAME = "com.linkedin.databus2:ownerId=1234321,dimension=eventsInbound.total,type=AggregatedDbusEventsTotalStats";
	public static final String RELAY_ADMIN_MBEAN_NAME = "com.linkedin.databus2:ownerId=1234321,name=HttpRelay,type=DatabusComponentAdmin";

	public static final String BOOTSTRAP_PRODUCER_OUTBOUND_STATS_MBEAN_NAME = "com.linkedin.databus2:ownerId=67677877,dimension=eventsOutbound.total,type=AggregatedDbusEventsTotalStats";
	public static final String BOOTSTRAP_PRODUCER_INBOUND_STATS_MBEAN_NAME = "com.linkedin.databus2:ownerId=67677877,dimension=eventsInbound.total,type=AggregatedDbusEventsTotalStats";
	public static final String BOOTSTRAP_PRODUCER_ADMIN_MBEAN_NAME = "com.linkedin.databus2:ownerId=67677877,name=DatabusHttpClientImpl,type=DatabusComponentAdmin";

	public static final String BOOTSTRAP_SERVER_OUTBOUND_STATS_MBEAN_NAME = "com.linkedin.databus2:ownerId=1234567,dimension=eventsOutbound.total,type=AggregatedDbusEventsTotalStats";
	public static final String BOOTSTRAP_SERVER_INBOUND_STATS_MBEAN_NAME = "com.linkedin.databus2:ownerId=1234567,dimension=eventsInbound.total,type=AggregatedDbusEventsTotalStats";

	public static final String BOOTSTRAP_SERVER_HTTP_OUTBOUND_STATS_MBEAN_NAME = "com.linkedin.databus2:ownerId=1234567,dimension=bootstrapHttpOutbound.total,type=DbusBootstrapHttpStats";

	public static final String BOOTSTRAP_SERVER_ADMIN_MBEAN_NAME = "com.linkedin.databus2:ownerId=1234567,name=BootstrapHttpServer,type=DatabusComponentAdmin";

	protected DbusEventsTotalStatsMBean _relayInStatsMBean;
	protected DbusEventsTotalStatsMBean _relayOutStatsMBean;
	protected DatabusComponentAdminMBean _relayAdminMBean;

	protected DbusEventsTotalStatsMBean _bootstrapServerInStatsMBean;
	protected DbusEventsTotalStatsMBean _bootstrapServerOutStatsMBean;
	protected DatabusComponentAdminMBean _bootstrapServerAdminMBean;
	protected DbusBootstrapHttpStatsMBean _bootstrapServerOutHttpStatsMBean;

	protected DbusEventsTotalStatsMBean _bootstrapProducerInStatsMBean;
	protected DbusEventsTotalStatsMBean _bootstrapProducerOutStatsMBean;
	protected DatabusComponentAdminMBean _bootstrapProducerAdminMBean;

	private String _relayConfigFile = RELAY_PROPERTY_NAME;
	protected String _bootstrapServiceConfigFile = BOOTSTRAP_PROPERTY_NAME;
	private String _bootstrapProducerConfigFile = BOOTSTRAP_PRODUCER_PROPERTY_NAME;
	private String _testName = DEFAULT_TESTNAME;

	protected IntegratedDummyDatabusConsumer _consumer = null;
	protected File _integrationDir;
	protected File _scriptDir;
	protected File _integrationVarDir;
	protected File _integrationVarLogDir;

	public void setUp() throws Exception {

        setupLogger();
		LOG.info("setUp: in");
		loadSystemProperties();
		reinit();
		// start relay and bootstrap services
		operateServices(SERVICE_OPERATION_START);
		LOG.info("Services started! " + new Date(System.currentTimeMillis()));
		LOG.info("Setup Complete: " + getTestName());
	}
	public void tearDown() throws Exception {
	  LOG.info("tearDown: in");
		shutdownConsumer();
		operateServices(SERVICE_OPERATION_STOP);
		operateBootstrapProducer(SERVICE_OPERATION_STOP);
		LOG.info("Test Complete" + getTestName());
	}

	protected void setRelayConfigFile(String relayConfigFile) {
		_relayConfigFile = relayConfigFile;
	}

	protected void setBootstrapServiceConfigFile(
			String bootstrapServiceConfigFile) {
		_bootstrapServiceConfigFile = bootstrapServiceConfigFile;
	}

	protected void setBootstrapProducerConfigFile(
			String bootstrapProducerConfigFile) {
		_bootstrapProducerConfigFile = bootstrapProducerConfigFile;
	}

	protected void setTestName(String testName) {
		_testName = testName;
	}

	protected String getTestName() {
		return _testName;
	}

	protected void loadSystemProperties() {
		Properties props = System.getProperties();
		_databusBaseDir = props.getProperty(DATABUS_BASED_DIR_TOKEN,
				DEFAULT_DATABUS_BASED_DIR);
		_integrationDir = new File(_databusBaseDir, "integration-test");
		_scriptDir = new File(_integrationDir, "script");
		_integrationVarDir = new File(_integrationDir, "var");
		_integrationVarLogDir = new File(_integrationVarDir, "log");
	}

	protected void reinit() throws IOException, InterruptedException,
			TimeoutException {
		ExternalCommand cmd = ExternalCommand.executeWithTimeout(_scriptDir,
				DRIVER_SCRIPT_NAME, EXEC_TIMEOUT, TESTNAME_OPTION_STR,
				_testName, COMPONENT_OPTION_STR, REINIT_COMPONENT);
		processCommandResult(cmd, REINIT_COMPONENT);
	}

	public void setupLogger() throws Exception {

		PatternLayout defaultLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c{1}} %m%n");
	    ConsoleAppender defaultAppender = new ConsoleAppender(defaultLayout);

	    Logger.getRootLogger().removeAllAppenders();
	    Logger.getRootLogger().addAppender(defaultAppender);

//	    Logger.getRootLogger().setLevel(Level.OFF);
//	    Logger.getRootLogger().setLevel(Level.ERROR);
//	    Logger.getRootLogger().setLevel(Level.WARN);
	    Logger.getRootLogger().setLevel(Level.INFO);

	    FileAppender fileAppender = null;
	    try
	    {
	      fileAppender = new FileAppender(defaultLayout, "integration-test/var/log/" + getTestName()+ ".txt");
	    }
	    catch (IOException io)
	    {
	      LOG.error(io);
	    }

	    if (null != fileAppender) Logger.getRootLogger().addAppender(fileAppender);

	    InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());

    }

	public void operateBootstrapProducer(String operation)
			throws MalformedObjectNameException, IOException,
			InterruptedException, TimeoutException {
		operateBootstrapProducer(operation, _bootstrapProducerConfigFile);
	}

	public void operateBootstrapProducer(String operation, String propertyName)
			throws IOException, InterruptedException, TimeoutException,
			MalformedObjectNameException {
		if (operation.equalsIgnoreCase("stop")) {
			closeJMXAgentHelper(_bootstrapProducerJMXHelper);
			_bootstrapProducerJMXHelper = null;
		}

		ExternalCommand cmd = ExternalCommand.executeWithTimeout(_scriptDir,
				DRIVER_SCRIPT_NAME, EXEC_TIMEOUT, TESTNAME_OPTION_STR,
				_testName, COMPONENT_OPTION_STR, BOOTSTRAP_PRODUCER_COMPONENT,
				SERVICE_OPERATION_OPTION_STR, operation,
				BOOTSTRAP_PRODUCER_OPTION_STR, propertyName
				);
		processCommandResult(cmd, BOOTSTRAP_PRODUCER_COMPONENT + " "
				+ operation);

		if (operation.equalsIgnoreCase("start")) {
			_bootstrapProducerJMXHelper = JMXAgentHelper.create(JMXAgentHelper
					.makeJMXAgentURLString("localhost", 1099, 11221));
			_bootstrapProducerOutStatsMBean = _bootstrapProducerJMXHelper
					.getMBeanProxy(
							BOOTSTRAP_PRODUCER_OUTBOUND_STATS_MBEAN_NAME,
							DbusEventsTotalStatsMBean.class);
			_bootstrapProducerInStatsMBean = _bootstrapProducerJMXHelper
					.getMBeanProxy(BOOTSTRAP_PRODUCER_INBOUND_STATS_MBEAN_NAME,
							DbusEventsTotalStatsMBean.class);
			_bootstrapProducerAdminMBean = _bootstrapProducerJMXHelper
					.getMBeanProxy(BOOTSTRAP_PRODUCER_ADMIN_MBEAN_NAME,
							DatabusComponentAdminMBean.class);
		}

	}

	protected void generateNumEvents(long numEvents, long fromScn,
			int eventPerSec, ArrayList<Integer> srcIds) throws IOException,
			InterruptedException, TimeoutException {
		StringBuilder strBuilder = new StringBuilder();
		for (Integer id : srcIds) {
			strBuilder.append(id);
			strBuilder.append(",");
		}
		// remove the last ","
		strBuilder.deleteCharAt(strBuilder.length() - 1);

		ExternalCommand cmd = ExternalCommand.executeWithTimeout(_scriptDir,
				WORKLOAD_GEN_SCRIPT,
				EXEC_TIMEOUT, FROM_SCN_OPTION_STR, String.valueOf(fromScn),
				EVENT_FREQUENCY_OPTION_STR, String.valueOf(eventPerSec),
				NUMBER_OF_EVENTS_OPTION_STR, String.valueOf(numEvents),
				SOURCES_LIST_OPTION_STR, String.valueOf(strBuilder.toString()),
				WAIT_TIL_SUSPEND_OPTION_STR);
		processCommandResult(cmd, WORKLOAD_GEN_SCRIPT);
	}

	protected void pauseWorkloadGen() throws IOException, InterruptedException,
			TimeoutException {
		ExternalCommand cmd = ExternalCommand.executeWithTimeout(_scriptDir,
				WORKLOAD_GEN_SCRIPT, EXEC_TIMEOUT, PAUSE_WORKLOAD_OPTION_STR);
		processCommandResult(cmd, WORKLOAD_GEN_SCRIPT);
	}

	protected void resumeWorkloadGen() throws IOException,
			InterruptedException, TimeoutException {
		ExternalCommand cmd = ExternalCommand.executeWithTimeout(_scriptDir,
				WORKLOAD_GEN_SCRIPT,
				EXEC_TIMEOUT, RESUME_WORKLOAD_OPTION_STR);
		processCommandResult(cmd, WORKLOAD_GEN_SCRIPT);
	}

	protected void resumeWorkloadGen(long numEvents) throws IOException,
			InterruptedException, TimeoutException {
		ExternalCommand cmd = ExternalCommand.executeWithTimeout(_scriptDir,
				WORKLOAD_GEN_SCRIPT,
				EXEC_TIMEOUT, RESUME_WORKLOAD_OPTION_STR,
				NUMBER_OF_EVENTS_OPTION_STR, String.valueOf(numEvents));
		processCommandResult(cmd, WORKLOAD_GEN_SCRIPT);
	}

	protected void resumeWorkloadGen(long numEvents, boolean waitForFinish)
			throws IOException, InterruptedException, TimeoutException {
		if (waitForFinish) {
			ExternalCommand cmd = ExternalCommand.executeWithTimeout(_scriptDir,
					WORKLOAD_GEN_SCRIPT, EXEC_TIMEOUT,
					RESUME_WORKLOAD_OPTION_STR, NUMBER_OF_EVENTS_OPTION_STR,
					String.valueOf(numEvents), WAIT_TIL_SUSPEND_OPTION_STR);
			processCommandResult(cmd, WORKLOAD_GEN_SCRIPT);
		} else {
			resumeWorkloadGen(numEvents);
		}
	}

	protected void checkResult(String clientFile, boolean useCompression)
			throws IOException, InterruptedException, TimeoutException {
		checkResult(clientFile, useCompression, null);
	}

	public void checkResult(String clientFile, boolean useCompression,
			List<String> ignoredList, long startScn, long endScn)
			throws IOException, InterruptedException, TimeoutException {
		ExternalCommand cmd;
		ArrayList<String> arguments = new ArrayList<String>();

		if (useCompression) {
			arguments.add("-c");
		}

		if (null != ignoredList) {
			Assert.assertFalse(0 == ignoredList.size(), "ignoredList is empty");
			arguments.add("-i");
			arguments.addAll(ignoredList);
		}

		arguments.add("--testname");
		arguments.add(getTestName());
		arguments.add("-s");
		arguments.add("integration-test/var/log/" + getTestName() + "/databus_relay_event_trace");
		arguments.add(clientFile);

		// add scn range for
		// arguments.add

		String[] argArray = arguments.toArray(new String[0]);
		cmd = ExternalCommand.executeWithTimeout(_scriptDir, RESULT_VERIFICATION_SCRIPT,
				EXEC_TIMEOUT, argArray);

		int exitValue = cmd.exitValue();

		if (0 != exitValue) {
			String errString = new String(cmd.getError());
			Assert.fail(errString);
		}
	}

	public void checkResult(String clientFile, boolean useCompression,
			List<String> ignoredList) throws IOException, InterruptedException,
			TimeoutException {
		checkResult(clientFile, useCompression, ignoredList, 0, Long.MAX_VALUE);
	}

	public void runCommandLineTest(String testName) throws IOException,
			InterruptedException, TimeoutException {
		ExternalCommand cmd;
		ArrayList<String> arguments = new ArrayList<String>();
		arguments.add(TESTCASE_OPTION_STR);
		arguments.add(testName);
		String[] argArray = arguments.toArray(new String[0]);
		cmd = ExternalCommand.executeWithTimeout(_scriptDir, DRIVER_SCRIPT_NAME, EXEC_TIMEOUT,
				argArray);

		int exitValue = cmd.exitValue();

		if (0 != exitValue) {
			// String errString = new String(cmd.getError());
			String errString = new String(cmd.getOutput());
			if (null != errString)
			{
  			  LOG.error("failed test (" + testName + ")output start: ###################");
  			  BufferedReader in = new BufferedReader(new StringReader(errString));
  			  String line = null;
  			  while ((line = in.readLine()) != null)
  			  {
  			    LOG.error(line);
  			  }
  			  in.close();
              LOG.error("failed test (" + testName + ")output end: ###################");
			}
			Assert.assertTrue(false, errString);
		}
		else
		{
		  LOG.info("test " + testName + " passed.");
		}
	}

	protected void waitForOutputDone(DbusEventsTotalStatsMBean outStats,
			long totalWork, long timeout) {
		boolean done = false;
		long sleptTime = 0;
		long sleepBeginTime = 0;

		while (!done) {
			try {
			    long outEvents = outStats.getNumDataEvents();
			    if (LOG.isDebugEnabled()) LOG.debug("waitForOutputDone: outEvents=" + outEvents);
				if (outEvents >= totalWork) { // done, exit
					done = true;
				} else if (timeout <= sleptTime) { // throw an error since
													// expected number of events
													// are not received
					Assert.fail("Timed out after waiting for " + sleptTime
							+ " seconds. Value expected (" + totalWork
							+ "), Value current ("
							+ outStats.getNumDataEvents() + ").");
				} else { // sleep for a bit and check again
				    LOG.debug("sleeping because threshold not reached.");
					sleepBeginTime = System.currentTimeMillis();
					Thread.sleep(100);
					sleptTime += System.currentTimeMillis() - sleepBeginTime;
				}
			} catch (InterruptedException e) {
				// go check stats again.
				sleptTime += System.currentTimeMillis() - sleepBeginTime;
			}
		}

	}

	protected void waitForInputDone(DbusEventsTotalStatsMBean inStats,
			long totalWork, long timeout) throws InterruptedException {
		boolean done = false;
		long sleptTime = 0;
		long sleepBeginTime = 0;

		boolean debugEnabled = LOG.isDebugEnabled();

		while (!done) {
			if (debugEnabled)
				LOG.debug("waitForInputDone: need " + totalWork + " have "
						+ inStats.getNumDataEvents());
			try {
				if (inStats.getNumDataEvents() >= totalWork) { // done, exit
					done = true;
				} else if (timeout < sleptTime) { // throw an error since
													// expected number of events
													// are not received
					Assert.fail("Timed out after waiting for " + sleptTime
							+ " seconds. Value expected (" + totalWork
							+ "), Value current (" + inStats.getNumDataEvents()
							+ ").");
				} else { // sleep for a bit and check again
					sleepBeginTime = System.currentTimeMillis();
					Thread.sleep(100);
					sleptTime += System.currentTimeMillis() - sleepBeginTime;
				}
			} catch (InterruptedException e) {
				// go check stats again.
				sleptTime += System.currentTimeMillis() - sleepBeginTime;
			}
		}

		// The following sleep is temporary because we currently has no way to
		// ensure
		// bootstrap applier has applied all events produced. w/o this sleep, a
		// lot
		// of integration tests will fail because client will start query
		// bootstrap
		// server too early. Get rid of it once we have jmx stats for applier
		// thread.
		Thread.sleep(10000);
	}

	protected void waitForStatus(DatabusComponentAdminMBean adminMBean,
			DatabusComponentStatus.Status expectedStatus, long timeout) {
		boolean done = false;
		long sleptTime = 0;
		long sleepBeginTime = 0;

		while (!done) {
			try {
				String currentStatusStr = adminMBean.getStatus();
				if (DatabusComponentStatus.Status.valueOf(currentStatusStr) == expectedStatus) { // done,
																									// exit
					done = true;
				} else if (timeout < sleptTime) { // throw an error since
													// expected number of events
													// are not received
					Assert.fail("Timed out after waiting for " + sleptTime
							+ " seconds. Value expected (" + expectedStatus
							+ "), Value current (" + adminMBean.getStatus()
							+ ").");
				} else { // sleep for a bit and check again
					sleepBeginTime = System.currentTimeMillis();
					Thread.sleep(100);
					sleptTime += System.currentTimeMillis() - sleepBeginTime;
				}
			} catch (InterruptedException e) {
				// go check stats again.
				sleptTime += System.currentTimeMillis() - sleepBeginTime;
			}
		}
	}

	protected void waitForConsumerRelayScn(long targetScn,
			IntegratedDummyDatabusConsumer consumer, long timeout) {
		boolean done = false;
		long sleptTime = 0;
		long sleepBeginTime = 0;

		while (!done) {
			try {
				if (consumer.getMaxRelayWindowScn() >= targetScn) { // done,
																	// exit
					done = true;
				} else if (timeout < sleptTime) { // throw an error since
													// expected number of events
													// are not received
					Assert.fail("Timed out after waiting for " + sleptTime
							+ " seconds. Target scn (" + targetScn
							+ "), Current scn ("
							+ consumer.getMaxRelayWindowScn() + ").");
				} else { // sleep for a bit and check again
					sleepBeginTime = System.currentTimeMillis();
					Thread.sleep(100);
					sleptTime += System.currentTimeMillis() - sleepBeginTime;
				}
			} catch (InterruptedException e) {
				// go check stats again.
				sleptTime += System.currentTimeMillis() - sleepBeginTime;
			}
		}
	}

	protected void waitForConsumerBootstrapScn(long targetScn,
			IntegratedDummyDatabusConsumer consumer, long timeout) {
		boolean done = false;
		long sleptTime = 0;
		long sleepBeginTime = 0;

		boolean debugEnabled = LOG.isDebugEnabled();

		while (!done) {
			try {
				if (consumer.getMaxBootstrapWindowScn() >= targetScn) { // done,
																		// exit
					done = true;
				} else if (timeout < sleptTime) { // throw an error since
													// expected number of events
													// are not received
					Assert.fail("Timed out after waiting for " + sleptTime
							+ " seconds. Target scn (" + targetScn
							+ "), Current scn ("
							+ consumer.getMaxBootstrapWindowScn() + ").");
				} else { // sleep for a bit and check again
				  if (debugEnabled) LOG.debug("waitForConsumerBootstrapScn: targetScn=" + targetScn
				                              + " bootstrapScn=" + consumer.getMaxBootstrapWindowScn());
					sleepBeginTime = System.currentTimeMillis();
					Thread.sleep(100);
					sleptTime += System.currentTimeMillis() - sleepBeginTime;
				}
			} catch (InterruptedException e) {
				// go check stats again.
				sleptTime += System.currentTimeMillis() - sleepBeginTime;
			}
		}
	}

	protected void shutdownConsumer() {
		if (null != _consumer) {
			_consumer.shutdown();
			_consumer = null;
		}
	}

	private void operateServices(String operation) throws IOException,
			InterruptedException, TimeoutException,
			MalformedObjectNameException {
      LOG.info("operateServices: in");

		ExternalCommand cmd = null;

		if (operation.equalsIgnoreCase("stop")) {
			closeJMXAgentHelper(_relayJMXHelper);
			closeJMXAgentHelper(_bootstrapServerJMXHelper);
			_relayJMXHelper = null;
			_bootstrapServerJMXHelper = null;
		}

		LOG.info("invoking " + operation + " on relay");

		cmd = ExternalCommand.executeWithTimeout(_scriptDir, DRIVER_SCRIPT_NAME,
				EXEC_TIMEOUT, TESTNAME_OPTION_STR, _testName,
				COMPONENT_OPTION_STR, RELAY_SERVICE_COMPONENT,
				SERVICE_OPERATION_OPTION_STR, operation,
				CMD_LINE_OPTION_STRING, "databus.relay.eventBuffer.trace.filename=integration-test/var/log/" + getTestName() + "/databus_relay_event_trace",
				RELAY_PROPERTY_OPTION_STR, _relayConfigFile);
		processCommandResult(cmd, RELAY_SERVICE_COMPONENT + " " + operation);

        LOG.info("invoking " + operation + " on bootstrap server");

		cmd = ExternalCommand.executeWithTimeout(_scriptDir, DRIVER_SCRIPT_NAME,
				EXEC_TIMEOUT, TESTNAME_OPTION_STR, _testName,
				COMPONENT_OPTION_STR, BOOTSTRAP_SERVICE_COMPONENT,
				SERVICE_OPERATION_OPTION_STR, operation,
				BOOTSTRAP_PROPERTY_OPTION_STR, _bootstrapServiceConfigFile);
		processCommandResult(cmd, BOOTSTRAP_SERVICE_COMPONENT + " " + operation);

        LOG.info("done invoking " + operation);

		if (operation.equalsIgnoreCase("start")) {
			// get mbean proxies for the above services
			_relayJMXHelper = JMXAgentHelper.create(JMXAgentHelper
					.makeJMXAgentURLString("localhost", 1099, 9999));
			_bootstrapServerJMXHelper = JMXAgentHelper.create(JMXAgentHelper
					.makeJMXAgentURLString("localhost", 1099, 7777));
			_relayOutStatsMBean = _relayJMXHelper.getMBeanProxy(
					RELAY_OUTBOUND_STATS_MBEAN_NAME,
					DbusEventsTotalStatsMBean.class);
			_relayInStatsMBean = _relayJMXHelper.getMBeanProxy(
					RELAY_INBOUND_STATS_MBEAN_NAME,
					DbusEventsTotalStatsMBean.class);
			_relayAdminMBean = _relayJMXHelper.getMBeanProxy(
					RELAY_ADMIN_MBEAN_NAME, DatabusComponentAdminMBean.class);

			_bootstrapServerOutStatsMBean = _bootstrapServerJMXHelper
					.getMBeanProxy(BOOTSTRAP_SERVER_OUTBOUND_STATS_MBEAN_NAME,
							DbusEventsTotalStatsMBean.class);
			_bootstrapServerInStatsMBean = _bootstrapServerJMXHelper
					.getMBeanProxy(BOOTSTRAP_SERVER_INBOUND_STATS_MBEAN_NAME,
							DbusEventsTotalStatsMBean.class);
			_bootstrapServerAdminMBean = _bootstrapServerJMXHelper
					.getMBeanProxy(BOOTSTRAP_SERVER_ADMIN_MBEAN_NAME,
							DatabusComponentAdminMBean.class);
			_bootstrapServerOutHttpStatsMBean = _bootstrapServerJMXHelper
					.getMBeanProxy(
							BOOTSTRAP_SERVER_HTTP_OUTBOUND_STATS_MBEAN_NAME,
							DbusBootstrapHttpStatsMBean.class);
		}
	    LOG.info("operateServices: out");
	}

	protected void processCommandResult(ExternalCommand cmd, String cmdName)
			throws InterruptedException {
		if (cmd.exitValue() != 0) {
			LOG.error(cmd.getStringOutput());
			Assert.fail(cmdName + " failed");
		}
	}

	protected void closeJMXAgentHelper(JMXAgentHelper helper) {
		if (null != helper) {
			try {
				helper.close();
			} catch (Exception e) {
				// don't need to do anything if we get an error closing it.
			}
		}
	}
}
