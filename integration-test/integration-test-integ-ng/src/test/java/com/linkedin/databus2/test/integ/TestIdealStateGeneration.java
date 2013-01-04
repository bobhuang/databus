package com.linkedin.databus2.test.integ;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus3.espresso.cmclient.RelayIdealStateGenerator;

public class TestIdealStateGeneration
{
	  public static final String MODULE = TestIdealStateGeneration.class.getName();
	  public static final Logger LOG = Logger.getLogger(MODULE);

	  private File _rootDir;
	  private File _integrationTestsDir;
	  private File _varDir;
	  private File _logDir;
	  private long _testTimestamp;

	  @BeforeClass
	  public void beforeClass() throws Exception
	  {
	    _testTimestamp = System.currentTimeMillis();

	    String rootDirPath = System.getProperty(TestIdealStateGeneration.class.getName() + ".rootDir");
	    if (null == rootDirPath || 0 == rootDirPath.trim().length()) rootDirPath = ".";
	    _rootDir = new File(rootDirPath);
	    _integrationTestsDir = new File(_rootDir, "integration-test");
	    _varDir = new File(_integrationTestsDir, "var");
	    _logDir = new File(_varDir, "log");
	    if (! _logDir.exists())
	    {
	      if (! _logDir.mkdirs()) throw new IOException("unable to create:" + _logDir.getAbsolutePath());
	    }

	    File testLogFile = new File(_logDir, "TestIdealStateGeneration_" + _testTimestamp);

	    Logger rootLogger = Logger.getRootLogger();
	    rootLogger.removeAllAppenders();
	    PatternLayout defLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c} %m%n");
	    Appender fileAppender = new RollingFileAppender(defLayout, testLogFile.getAbsolutePath());
	    rootLogger.addAppender(fileAppender);
	    rootLogger.setLevel(Level.DEBUG);
	    
	    LOG.info("Finished beforeClass for TestIdealStateGeneration");
	  }

	  @AfterClass
	  public void afterClass()
	  {
		  LOG.info("Finished afterClass for TestIdealStateGeneration");
	  }
	  
	  private void setLogFile(String fileNameSuffix)
	  {
		  try 
		  {
			  File testLogFile = new File(_logDir, "TestIdealStateGeneration_"
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
	  public void testIdealStateGenLocally() throws Exception
	  {
		  setLogFile("testIdealStateGenLocally");
		  LOG.info("Running test testIdealStateGenLocally");
		  /**
		   * Create relay i.s.g
		   */
		  MockIdealStateGenerator risg = new MockIdealStateGenerator();
		  
		  /**
		   * Obtain Relay Ideal State by computing locally
		   */
		  risg.obtainRelayIdealState();
	  }
	  	  
	  @Test
	  public void testIdealStateGenWithRelayCluster() throws Exception
	  {
		  setLogFile("testIdealStateGenWithRelayCluster");
		  LOG.info("Running test testIdealStateGenWithRelayCluster");
		  /** 
		   * Storage node parameters
		   */
		  
		  final String snZkHost = "esv4-app66.stg.linkedin.com";
		  final int snZkPort = 2181;
		  final String snClusterName = "ESPRESSO_BIZPROFILE";

		  final String snZkConnectString = snZkHost + ":" + snZkPort;

		  /**
		   * Relay-side parameters
		   */
		  final int relayReplicationFactor = 1;

		  /**
		   * Relay cluster parameters ( to obtain list of relayNodes )
		   */
		  String relayZkConnectString = "eat1-app207.stg.linkedin.com:12913";
          String relayClusterName = "RelayIntegrationTestCluster";

		  try {
			  LOG.info("Creating Relay Ideal State Generator");
			  RelayIdealStateGenerator risg = new RelayIdealStateGenerator(relayReplicationFactor,
					                                                       snZkConnectString, snClusterName, 
					                                                       relayZkConnectString, relayClusterName, null);
			  risg.connect(false);

			  LOG.info("Calculating final ideal state - by joining initial ideal state with s.ext view");
			  risg.writeIdealStatesForCluster();
		  } catch (Exception e)
		  {
			  LOG.info("Got exception :", e);
			  throw e;
		  }
		  return;		  
	  }
	  
	  @Test
	  public void testIdealStateGenWithTwoDatabases() throws Exception
	  {
		  setLogFile("testIdealStateGenWithTwoDatabases");
		  LOG.info("Running test testIdealStateGenWithTwoDatabases");
		  /** 
		   * Storage node parameters
		   */
		  
		  final String snZkHost = "eat1-app207.stg.linkedin.com";
		  final int snZkPort = 12913;
		  final String snClusterName = "ESPRESSO_DEV_FT_1";

		  final String snZkConnectString = snZkHost + ":" + snZkPort;

		  /**
		   * Relay-side parameters
		   */
		  final int relayReplicationFactor = 1;

		  /**
		   * Relay cluster parameters ( to obtain list of relayNodes )
		   */
		  String relayZkConnectString = "eat1-app207.stg.linkedin.com:12913";
          String relayClusterName = "RelayIntegrationTestClusterUSCP";

		  try {
			  LOG.info("Creating Relay Ideal State Generator");
			  RelayIdealStateGenerator risg = new RelayIdealStateGenerator(relayReplicationFactor,
					                                                       snZkConnectString, snClusterName, 
					                                                       relayZkConnectString, relayClusterName, null);
			  risg.connect(false);

			  LOG.info("Calculating final ideal state - by joining initial ideal state with s.ext view");
			  risg.writeIdealStatesForCluster();
		  } catch (Exception e)
		  {
			  LOG.info("Got exception :", e);
			  throw e;
		  }
		  return;		  
	  }

}
