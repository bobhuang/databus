package com.linkedin.databus2.test.integ;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.xeril.util.TimeOutException;
import org.xeril.util.thread.ExternalCommand;

import com.linkedin.databus2.test.ConditionCheck;
import com.linkedin.databus2.test.TestUtil;

@Test(singleThreaded=true)
public class TestDeployment
{
  public static final String MODULE = TestDeployment.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private static final long MINUTE_IN_MILLIS = 60000;
  private static final HashMap<String, Integer> SERVICE_PORTS = new HashMap<String, Integer>();
  static
  {
    SERVICE_PORTS.put("anet_relay_deploy", 11269);
    SERVICE_PORTS.put("bizfollow_relay_deploy", 9190);
    SERVICE_PORTS.put("cappr_relay_deploy", 11351);
    SERVICE_PORTS.put("conn_relay_deploy", 11441);
    SERVICE_PORTS.put("following_relay_deploy", 9196);
    SERVICE_PORTS.put("forum_relay_deploy", 11246);
    SERVICE_PORTS.put("liar_relay_deploy", 9091);
    SERVICE_PORTS.put("mbrrec_relay_deploy", 11322);
    SERVICE_PORTS.put("member2_relay_deploy", 11183);
    SERVICE_PORTS.put("news_relay_deploy", 11441);
    SERVICE_PORTS.put("anet_bst_producer_deploy", 11271);
    SERVICE_PORTS.put("bizfollow_bst_producer_deploy", 6060);
    SERVICE_PORTS.put("cappr_bst_producer_deploy", 11352);
    SERVICE_PORTS.put("conn_bst_producer_deploy", 11273);
    SERVICE_PORTS.put("following_bst_producer_deploy", 11285);
    SERVICE_PORTS.put("forum_bst_producer_deploy", 11244);
    SERVICE_PORTS.put("liar_bst_producer_deploy", 6061);
    SERVICE_PORTS.put("mbrrec_bst_producer_deploy", 11277);
    SERVICE_PORTS.put("member2_bst_producer_deploy", 11185);
    SERVICE_PORTS.put("news_bst_producer_deploy", 11332);
    SERVICE_PORTS.put("bootstrap_server_deploy", 6161);
  }

  private File _rootDir;
  private File _integrationTestsDir;
  private File _scriptsDir;
  private File _varDir;
  private File _logDir;
  private long _testTimestamp;

  @BeforeClass
  public void beforeClass() throws Exception
  {
    _testTimestamp = System.currentTimeMillis();

    String rootDirPath = System.getProperty(TestDeployment.class.getName() + ".rootDir");
    if (null == rootDirPath || 0 == rootDirPath.trim().length()) rootDirPath = ".";
    _rootDir = new File(rootDirPath);
    _integrationTestsDir = new File(_rootDir, "integration-test");
    _scriptsDir = new File(_integrationTestsDir, "script");
    _varDir = new File(_integrationTestsDir, "var");
    _logDir = new File(_varDir, "log");
    if (! _logDir.exists())
    {
      if (! _logDir.mkdirs()) throw new IOException("unable to create:" + _logDir.getAbsolutePath());
    }

    File testLogFile = new File(_logDir, "TestDeployment_" + _testTimestamp);

    Logger rootLogger = Logger.getRootLogger();
    rootLogger.removeAllAppenders();
    PatternLayout defLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c} %m%n");
    Appender fileAppender = new RollingFileAppender(defLayout, testLogFile.getAbsolutePath());
    rootLogger.addAppender(fileAppender);
    rootLogger.setLevel(Level.INFO);
    rootLogger.setLevel(Level.DEBUG);
  }

  @AfterClass
  public void afterClass()
  {
  }

  @Test
  public void testAnetDeployment() throws IOException, TimeOutException, InterruptedException
  {
    runFullDeploymentTest("anet");
  }

  @Test
  public void testBizfollowDeployment() throws IOException, TimeOutException, InterruptedException
  {
    runFullDeploymentTest("bizfollow");
  }

  @Test
  public void testCapprDeployment() throws IOException, TimeOutException, InterruptedException
  {
    runFullDeploymentTest("cappr");
  }

  @Test
  public void testConnDeployment() throws IOException, TimeOutException, InterruptedException
  {
    runFullDeploymentTest("conn");
  }


  @Test
  public void testFollowingDeployment() throws IOException, TimeOutException, InterruptedException
  {
    runFullDeploymentTest("following");
  }

  @Test
  public void testForumDeployment() throws IOException, TimeOutException, InterruptedException
  {
    runFullDeploymentTest("forum");
  }

  @Test
  public void testLiarDeployment() throws IOException, TimeOutException, InterruptedException
  {
    runFullDeploymentTest("liar");
  }

  @Test
  public void testMbrrecDeployment() throws IOException, TimeOutException, InterruptedException
  {
    runFullDeploymentTest("mbrrec");
  }

  @Test
  public void testMember2Deployment() throws IOException, TimeOutException, InterruptedException
  {
    runFullDeploymentTest("member2");
  }

  @Test
  public void testNewsDeployment() throws IOException, TimeOutException, InterruptedException
  {
    runFullDeploymentTest("news");
  }

  @Test
  public void testBootstrapServerDeployment() throws IOException, TimeOutException, InterruptedException
  {
    undeploy("bootstrap_server_deploy");
    deploy("bootstrap_server_deploy");
    undeploy("bootstrap_server_deploy");
  }

  private void deploy(final String name) throws IOException, TimeOutException, InterruptedException
  {
    LOG.info("deploying " + name + " ...");
    ExternalCommand deployCmd = runDriverScript("-c", name, "-o", "start");
    assertServerDeployed(name);
    LOG.info(name + " deployed");
    deployCmd.destroy();
  }

  private void undeploy(final String name) throws IOException, TimeOutException, InterruptedException
  {
    if (! TestUtil.checkServerRunning("localhost", SERVICE_PORTS.get(name), LOG, false))
    {
      LOG.info(name + " not running.");
      return;
    }

    LOG.info("undeploying " + name);
    ExternalCommand undeployCmd = runDriverScript("-c", name, "-o", "stop");
    assertServerUndeployed(name);
    LOG.info(name + " undeployed");
    undeployCmd.destroy();
  }

  private void runFullDeploymentTest(String name) throws IOException, TimeOutException, InterruptedException
  {
    stopBootstrapProducer(name);
    stopRelay(name);
    startRelay(name);
    startBootstrapProducer(name);
    stopBootstrapProducer(name);
    stopRelay(name);
  }

  private void startRelay(final String name) throws IOException, TimeOutException, InterruptedException
  {
    deploy(name + "_relay_deploy");
  }

  private void stopRelay(final String name) throws IOException, TimeOutException, InterruptedException
  {
    undeploy(name + "_relay_deploy");
  }

  private void startBootstrapProducer(final String name) throws IOException, TimeOutException, InterruptedException
  {
    deploy(name + "_bst_producer_deploy");
  }

  private void stopBootstrapProducer(final String name) throws IOException, TimeOutException, InterruptedException
  {
    undeploy(name + "_bst_producer_deploy");
  }

  private ExternalCommand runDriverScript(String... args)
          throws IOException, TimeOutException, InterruptedException
  {
    boolean isDebugEnabled = LOG.isDebugEnabled();
    ExternalCommand result =
        ExternalCommand.executeWithTimeout(_scriptsDir, "dbus2_driver.py",
                                           1 * MINUTE_IN_MILLIS, args);

    if (isDebugEnabled || 0 != result.exitValue())
    {
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

  private List<String> findErrors(File file, int skipLines) throws IOException
  {
    ArrayList<String> errors = new ArrayList<String>();
    int lineCount = 0;
    FileReader fileReader = new FileReader(file);
    try
    {
      BufferedReader lineReader = new BufferedReader(fileReader);
      try
      {
        String line;
        while ((null != (line = lineReader.readLine())) && lineCount < skipLines) ++lineCount;

        //log file rolled over
        if (lineCount < skipLines) return findErrors(file, 0);

        while ((null != (line = lineReader.readLine())))
        {
          ++lineCount;
          if (line.contains("ERROR") || line.contains("FATAL"))
          {
            errors.add(Integer.toString(lineCount) + ":" + line);
          }
        }
      }
      finally
      {
        lineReader.close();
      }
    }
    finally
    {
      fileReader.close();
    }

    return errors;
  }

  private int countLines(File file) throws IOException
  {
    if (!file.exists()) return 0;

    int lineCount = 0;
    FileReader fileReader = new FileReader(file);
    try
    {
      BufferedReader lineReader = new BufferedReader(fileReader);
      try
      {
        String line;
        while (null != (line = lineReader.readLine())) ++lineCount;
      }
      finally
      {
        lineReader.close();
      }
    }
    finally
    {
      fileReader.close();
    }

    return lineCount;
  }

  protected static void assertServerDeployed(final String name)
  {
    TestUtil.assertWithBackoff(new ConditionCheck()
      {
        @Override
        public boolean check()
        {return TestUtil.checkServerRunning("localhost", SERVICE_PORTS.get(name), LOG, false);}
      },
      name + "  responding",
      2 * MINUTE_IN_MILLIS,
      LOG
    );
  }

  protected static void assertServerUndeployed(final String name)
  {
    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check()
      {return !TestUtil.checkServerRunning("localhost", SERVICE_PORTS.get(name), LOG, false);}
    },
    name + "  not responding",
    2 * MINUTE_IN_MILLIS,
    LOG
    );
  }
}
