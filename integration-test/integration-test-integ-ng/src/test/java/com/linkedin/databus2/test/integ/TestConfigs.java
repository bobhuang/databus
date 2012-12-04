package com.linkedin.databus2.test.integ;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.xeril.util.TimeOutException;
import org.xeril.util.thread.ExternalCommand;

/** Tests configs for different environments */
public class TestConfigs
{
  public static final String MODULE = TestConfigs.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private static final long MINUTE_IN_MILLIS = 60000;

  private File _rootDir;
  private long _testTimestamp;
  private File _integrationTestsDir;
  private File _varDir;
  private File _logDir;
  private File _cfgBasedir;
  private File _cfgDir;

  @BeforeClass
  public void setUp() throws IOException, InterruptedException, TimeOutException
  {
    //setup logging
    _testTimestamp = System.currentTimeMillis();

    String rootDirPath = System.getProperty(TestConfigs.class.getName() + ".rootDir");
    if (null == rootDirPath || 0 == rootDirPath.trim().length()) rootDirPath = ".";
    _rootDir = new File(rootDirPath);
    _integrationTestsDir = new File(_rootDir, "integration-test");
    _varDir = new File(_integrationTestsDir, "var");
    _logDir = new File(_varDir, "log");
    if (! _logDir.exists())
    {
      if (! _logDir.mkdirs()) throw new IOException("unable to create:" + _logDir.getAbsolutePath());
    }

    File testLogFile = new File(_logDir, "TestConfigs_" + _testTimestamp);

    Logger rootLogger = Logger.getRootLogger();
    rootLogger.removeAllAppenders();
    PatternLayout defLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c} %m%n");
    Appender fileAppender = new RollingFileAppender(defLayout, testLogFile.getAbsolutePath());
    rootLogger.addAppender(fileAppender);
    rootLogger.setLevel(Level.INFO);
    rootLogger.setLevel(Level.DEBUG);

    //setup cfg2
    _cfgBasedir = new File(_logDir.getAbsolutePath());
    _cfgDir = new File(_cfgBasedir, "databus2-configs");
    if (! _cfgBasedir.exists())
    {
      if (! _cfgBasedir.mkdirs())
      {
        throw new IOException("Unable to create: " + _cfgBasedir.getAbsolutePath());
      }
    }

    checkoutCfg2();
  }

  @Test
  public void testRelayAnetConfigs() throws Exception
  {
    //List fabrics until we launch in all fabrics
    checkServiceConfigs("databus2-relay-anet", "PROD-ELA4", "STG-BETA");
  }

  @Test
  public void testRelayBizfollowConfigs() throws Exception
  {
    checkServiceConfigs("databus2-relay-bizfollow");
  }

  @Test
  public void testRelayConnConfigs() throws Exception
  {
    //List fabrics until we launch in all fabrics
    checkServiceConfigs("databus2-relay-conn", "PROD-ELA4", "STG-BETA");
  }

  @Test
  public void testRelayForumConfigs() throws Exception
  {
    checkServiceConfigs("databus2-relay-forum");
  }

  @Test
  public void testRelayLiarConfigs() throws Exception
  {
    checkServiceConfigs("databus2-relay-liar");
  }

  private void checkoutCfg2() throws IOException, InterruptedException, TimeOutException
  {
    ExternalCommand checkoutResult = null;
    if (! _cfgDir.exists())
    {
      checkoutResult = cfg2("checkout", "databus2");
    }
    else
    {
      checkoutResult = cfg2("up");
    }
    if (checkoutResult.exitValue() != 0) throw new IOException("cfg2 checkout failed");
  }

  private ExternalCommand cfg2(String ... args) throws IOException, InterruptedException, TimeOutException
  {
    boolean isDebugEnabled = LOG.isDebugEnabled();
    LOG.info("running cfg2 " + Arrays.toString(args));

    File workDir = args[0].equals("checkout") ? _cfgBasedir : _cfgDir;

    String[] realArgs = new String[args.length + 1];
    realArgs[0] = "/usr/local/linkedin/bin/cfg2";
    System.arraycopy(args, 0, realArgs, 1, args.length);
    ExternalCommand result = new ExternalCommand(new ProcessBuilder(realArgs));
    result.setRedirectErrorStream(true);
    result.setWorkingDirectory(workDir);

    result.start();
    result.waitFor(2 * MINUTE_IN_MILLIS);

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

  private void checkServiceConfigs(final String service) throws IOException, TimeOutException, InterruptedException
  {
    checkServiceConfigs(service, "PROD-ELA4", "PROD-ECH3", "STG-BETA", "EI");
  }

  private void checkServiceConfigs(final String service, final String... fabrics) throws IOException, TimeOutException, InterruptedException
  {
    for (String f: fabrics)
    {
      checkConfig(service, f);
    }
  }


  private void checkConfig(final String service, final String fabric) throws IOException, TimeOutException, InterruptedException
  {
    LOG.info("verifying config for " + service + " in " + fabric + " ...");
    ExternalCommand cmd = cfg2("publish", service, "-f", fabric, "--dry-run");
    Assert.assertEquals(cmd.exitValue(), 0, "verification for " + service + " in " + fabric);
  }

}
