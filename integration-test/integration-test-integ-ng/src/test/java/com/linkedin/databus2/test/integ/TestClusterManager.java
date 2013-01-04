package com.linkedin.databus2.test.integ;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.testng.annotations.Test;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.databus.test.ExternalCommand;;

/**
 * A test utility for testing out Cluster Manager functionality
 * 
 */

public class TestClusterManager {
	public static final String MODULE = TestClusterManager.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);

	private static final long MINUTE_IN_MILLIS = 60000;
	private static final HashMap<String, Integer> SERVICE_PORTS = new HashMap<String, Integer>();
	static {
		SERVICE_PORTS.put("espresso_relay", 10015);
	}

	private File _rootDir;
	private File _integrationTestsDir;
	private File _scriptsDir;
	private File _varDir;
	private File _logDir;
	private long _testTimestamp;

	@BeforeClass
	public void beforeClass() throws Exception {
		_testTimestamp = System.currentTimeMillis();

		String rootDirPath = System.getProperty(TestClusterManager.class
				.getName() + ".rootDir");
		if (null == rootDirPath || 0 == rootDirPath.trim().length())
			rootDirPath = ".";
		_rootDir = new File(rootDirPath);
		_integrationTestsDir = new File(_rootDir, "integration-test");
		_scriptsDir = new File(_integrationTestsDir, "script");
		_varDir = new File(_integrationTestsDir, "var");
		_logDir = new File(_varDir, "log");
		if (!_logDir.exists()) {
			if (!_logDir.mkdirs())
				throw new IOException("unable to create:"
						+ _logDir.getAbsolutePath());
		}
		
		setLogFile(null);
	}

	@AfterClass
	public void afterClass() {
	}

	private void setLogFile(String fileNameSuffix)
	{
		try 
		{
			File testLogFile = new File(_logDir, "TestClusterManager_"
					+ fileNameSuffix + _testTimestamp);

			Logger rootLogger = Logger.getRootLogger();
			rootLogger.removeAllAppenders();
			PatternLayout defLayout = new PatternLayout(
					"%d{ISO8601} +%r [%t] (%p) {%c} %m%n");
			Appender fileAppender = new RollingFileAppender(defLayout,
					testLogFile.getAbsolutePath());
			rootLogger.addAppender(fileAppender);
			rootLogger.setLevel(Level.INFO);
		} catch (Exception e)
		{}
	}

	@Test
	public void testTeamMoneyRelayCluster() throws Exception {
		final String relayClusterName = "testTeamMoneyRelayCluster";
		setLogFile(relayClusterName);
		CMZooKeeperServer zkServer = null;
		Thread t = null;
		try {
			LOG.info("=== BEGIN TEST : testTeamMoneyCluster ===");
			final int zkPort = 21810;
			final int ports[] = { 10015 };

			LOG.info("Starting ZK server");
			zkServer = new CMZooKeeperServer(zkPort, relayClusterName);
			zkServer.start();

			LOG.info("Add cluster " + relayClusterName);
			CMTools.addCluster(zkPort, relayClusterName);

			String nodeName = "localhost:" + Integer.toString(ports[0]);
			LOG.info("Add node " + nodeName);
			CMTools.addNode(zkPort, relayClusterName, nodeName);

			LOG.info("Write IdealState on ClusterManager");
			final List<String> storageNodes = new ArrayList<String>(
					Arrays.asList("esv4-app74.stg.linkedin.com_12001",
							"esv4-app76.stg.linkedin.com_12001",
							"esv4-app77.stg.linkedin.com_12001"));
			ZNRecord zr = CMTools.generateRelayIdealState(zkPort,
					relayClusterName, zkServer, storageNodes);

			ZNRecordSerializer serializer = new ZNRecordSerializer();
			LOG.info("====Generated IdealState===== ");
			LOG.info(new String(serializer.serialize(zr)));
			LOG.info("==============================");

			LOG.info("Starting cluster manager");
			t = CMTools.startClusterManager(zkPort, relayClusterName);

			LOG.info("Starting relay process on port "
					+ Integer.toString(ports[0]));
			startEspressoRelay(relayClusterName);

			LOG.info("Verifying state on zookeeper");
			int count = 0;
			final int NUM_RETRIES = 10;
			boolean rs = false;
			while(count++ <= NUM_RETRIES && (rs != true))
			{
				rs = CMTools.verifyState(zkPort, relayClusterName);
				LOG.info("Verify state returned " + Boolean.toString(rs) + " count = " + count);
				Thread.sleep(60000);
			}
			if (rs != true){
				throw new Exception("ClusterState verify came out as false");
			}
			LOG.info("Stopping Espresso relay in normal flow");
			stopEspressoRelay();
		} catch (Exception e) {
			LOG.info("Cleaning up - so that there are no cascading failures on other tests", e);
			stopEspressoRelay();
			/// Throw to report a failure
			throw e;
		}
		finally {
			try {
				LOG.info("Stopping clustermanager");
				t.interrupt();

				LOG.info("Shutdown zookeeper");
				zkServer.shutdown();
			} catch (Exception e){}
		}

	}

	
	@Test
	public void testDevRelayCluster() throws Exception {
		final String relayClusterName = "testDevRelayCluster";
		setLogFile(relayClusterName);
		CMZooKeeperServer zkServer = null;
		Thread t = null;
		try {
			LOG.info("=== BEGIN TEST : testDevRelayCluster ===");
			final int zkPort = 21810;
			final int ports[] = { 10015 };

			LOG.info("Starting ZK server");
			zkServer = new CMZooKeeperServer(zkPort, relayClusterName);
			zkServer.start();

			LOG.info("Add cluster " + relayClusterName);
			CMTools.addCluster(zkPort, relayClusterName);

			String nodeName = "localhost:" + Integer.toString(ports[0]);
			LOG.info("Add node " + nodeName);
			CMTools.addNode(zkPort, relayClusterName, nodeName);

			LOG.info("Adding resource group RelayLeaderStandy");
			String resourceGroupName = "relayLeaderStandby";
			CMTools.addResourceGroup(zkPort, relayClusterName, resourceGroupName, 1, "LeaderStandby" );
			CMTools.rebalance(zkPort, relayClusterName, resourceGroupName, 1);

			String dbName = "BizProfile";
			CMTools.addResourceGroup(zkPort, relayClusterName, dbName, 64, "OnlineOffline" );

			LOG.info("Starting cluster manager");
			t = CMTools.startClusterManager(zkPort, relayClusterName);

			LOG.info("Starting relay process on port "
					+ Integer.toString(ports[0]));

			startEspressoRelay(relayClusterName);

			int count = 0;
			final int NUM_RETRIES = 10;
			boolean rs = false;
			while(count++ <= NUM_RETRIES && (rs != true))
			{
				rs = CMTools.verifyState(zkPort, relayClusterName);
				LOG.info("Verify state returned " + Boolean.toString(rs) + " count = " + count );
				Thread.sleep(60000);
			}
			if (rs != true){
				throw new Exception("ClusterState verify came out as false");
			}
			LOG.info("Stopping Espresso relay in normal flow");
			stopEspressoRelay();
		} catch (Exception e) {
			LOG.info("Cleaning up - so that there are no cascading failures on other tests", e);
			stopEspressoRelay();
			/// Throw to report a failure
			throw e;
		} finally {
			try {
				LOG.info("Stopping clustermanager");
				t.interrupt();

				LOG.info("Shutdown zookeeper");
                                zkServer.shutdown();
			} catch (Exception e){}
		}
	}


	private void startEspressoRelay(String testname) throws IOException, InterruptedException, Exception {
		String name = "espresso_relay";
		LOG.info("Starting " + name + " ...");
		String pp = _rootDir
				+ "/databus3-relay/databus3-relay-cmdline-pkg/config/espresso_relay_integration.properties";
		String lp = _rootDir
				+ "/databus3-relay/databus3-relay-cmdline-pkg/config/espresso_relay_log4j_integ.properties";
                String clusterNameParams = "databus.relay.clusterManager.relayClusterName=" + testname;
		LOG.info("Relay properties path: " + pp);
		LOG.info("log4j properties path: " + lp);
		runDriverScript("-c", name, "-o", "start", "-p", pp, "-l", lp, "--testname", testname, "--cmdline_props", clusterNameParams);
//				,"--jvm_args", "-agentlib:jdwp=transport=dt_socket,suspend=n,address=8990,server=y");
		LOG.info(name + " Started");
	}

	private void stopEspressoRelay() throws IOException, Exception,
			InterruptedException {
		String name = "espresso_relay";
		LOG.info("Stopping " + name);
		runDriverScript("-c", name, "-o", "stop");
		LOG.info(name + " stopped");
	}
	
	private ExternalCommand runDriverScript(String... args) throws IOException,
			Exception, InterruptedException {
		boolean isDebugEnabled = LOG.isDebugEnabled();
		
		ExternalCommand result = ExternalCommand.executeWithTimeout(
				_scriptsDir, "dbus2_driver.py", 10 * MINUTE_IN_MILLIS, args);

		if (isDebugEnabled || 0 != result.exitValue()) {
			Level l = 0 == result.exitValue() ? Level.DEBUG : Level.ERROR;
			LOG.log(l, "========== OUTPUT ==========");
			LOG.log(l, result.getStringOutput());
			LOG.log(l, "========== END OF OUTPUT ==========");

			LOG.log(l, "========== ERROR ==========");
			LOG.log(l, result.getStringError());
			LOG.log(l, "========== END OF ERROR ==========");
		}
		return result;
	}

}
