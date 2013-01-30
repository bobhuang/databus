package com.linkedin.databus3.espresso.cmclient;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestStorageAdapter
{
	  public static final String MODULE = TestStorageAdapter.class.getName();
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

	    String rootDirPath = System.getProperty(TestStorageAdapter.class.getName() + ".rootDir");
	    if (null == rootDirPath || 0 == rootDirPath.trim().length()) rootDirPath = ".";
	    _rootDir = new File(rootDirPath);
	    _integrationTestsDir = new File(_rootDir, "integration-test");
	    _varDir = new File(_integrationTestsDir, "var");
	    _logDir = new File(_varDir, "log");
	    if (! _logDir.exists())
	    {
	      if (! _logDir.mkdirs()) throw new IOException("unable to create:" + _logDir.getAbsolutePath());
	    }

	    File testLogFile = new File(_logDir, "TestStorageAdapter_" + _testTimestamp);

	    Logger rootLogger = Logger.getRootLogger();
	    rootLogger.removeAllAppenders();
	    PatternLayout defLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c} %m%n");
	    Appender fileAppender = new RollingFileAppender(defLayout, testLogFile.getAbsolutePath());
	    rootLogger.addAppender(fileAppender);
	    rootLogger.setLevel(Level.DEBUG);
	    
	    LOG.info("Finished beforeClass for TestStorageAdapter");
	  }

	  @AfterClass
	  public void afterClass()
	  {
		  LOG.info("Finished afterClass for TestStorageAdapter");
	  }
	  
	  private void setLogFile(String fileNameSuffix)
	  {
		  try 
		  {
			  File testLogFile = new File(_logDir, "TestStorageAdapter_"
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
	  
	  private Map<String, Map<String, String>> testIdealStateGenAlgo1() throws Exception
	  {
		  setLogFile("testIdealStateGenAlgo1");
		  final List<String> storageNodes = 
				  new ArrayList<String>(Arrays.asList(
						  "eat1-storage1.stg.linkedin.com_11140",
						  "eat1-storage2.stg.linkedin.com_11140"
				  ));
		  final List<String> relayNodes = 
				  new ArrayList<String>(Arrays.asList(
						  "eat1-relay1.stg.linkedin.com_11140",
						  "eat1-relay2.stg.linkedin.com_11140",
						  "eat1-relay3.stg.linkedin.com_11140",
						  "eat1-relay4.stg.linkedin.com_11140",
						  "eat1-relay5.stg.linkedin.com_11140",
						  "eat1-relay6.stg.linkedin.com_11140"
						  ));

		  int relayReplicationFactor = 2;
		  Map<String, Map<String, String>> cis = StorageAdapter.getCalculatedIdealState(relayNodes, storageNodes, relayReplicationFactor);

		  return cis;
	  }

	  private Map<String, Map<String, String>> testIdealStateGenAlgo2() throws Exception
	  {
		  setLogFile("testIdealStateGenAlgo1");
		  final List<String> storageNodes = 
				  new ArrayList<String>(Arrays.asList(
						  "eat1-storage2.stg.linkedin.com_11140",
						  "eat1-storage1.stg.linkedin.com_11140"
				  ));
		  final List<String> relayNodes = 
				  new ArrayList<String>(Arrays.asList(
						  "eat1-relay1.stg.linkedin.com_11140",
						  "eat1-relay2.stg.linkedin.com_11140",
						  "eat1-relay3.stg.linkedin.com_11140",
						  "eat1-relay4.stg.linkedin.com_11140",
						  "eat1-relay5.stg.linkedin.com_11140",
						  "eat1-relay6.stg.linkedin.com_11140"
						  ));

		  int relayReplicationFactor = 2;
		  Map<String, Map<String, String>> cis = StorageAdapter.getCalculatedIdealState(relayNodes, storageNodes, relayReplicationFactor);

		  return cis;
	  }

	  private Map<String, Map<String, String>> testIdealStateGenAlgo3() throws Exception
	  {
		  setLogFile("testIdealStateGenAlgo1");
		  final List<String> storageNodes = 
				  new ArrayList<String>(Arrays.asList(
						  "eat1-storage2.stg.linkedin.com_11140",
						  "eat1-storage1.stg.linkedin.com_11140"
				  ));
		  final List<String> relayNodes = 
				  new ArrayList<String>(Arrays.asList(
						  "eat1-relay2.stg.linkedin.com_11140",
						  "eat1-relay3.stg.linkedin.com_11140",
						  "eat1-relay5.stg.linkedin.com_11140",
						  "eat1-relay4.stg.linkedin.com_11140",
						  "eat1-relay6.stg.linkedin.com_11140",
						  "eat1-relay1.stg.linkedin.com_11140"
						  ));

		  int relayReplicationFactor = 2;
		  Map<String, Map<String, String>> cis = StorageAdapter.getCalculatedIdealState(relayNodes, storageNodes, relayReplicationFactor);

		  return cis;
	  }
	  
	  @Test
	  public void validateInitialIdealStateGeneration() throws Exception
	  {
		  Map<String, Map<String, String>> cis1 = testIdealStateGenAlgo1();
		  Map<String, Map<String, String>> cis2 = testIdealStateGenAlgo2();
		  Map<String, Map<String, String>> cis3 = testIdealStateGenAlgo3();

		  Assert.assertEquals(cis1.equals(cis2), true);
		  Assert.assertEquals(cis3.equals(cis2), true);
	  }

}


