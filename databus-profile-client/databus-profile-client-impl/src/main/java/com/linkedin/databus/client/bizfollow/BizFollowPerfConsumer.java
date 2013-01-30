package com.linkedin.databus.client.bizfollow;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.generic.ConsumerPauseRequestProcessor;
import com.linkedin.databus.client.pub.ServerInfo.ServerInfoBuilder;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus2.core.container.request.ProcessorRegistrationConflictException;

public class BizFollowPerfConsumer
{
  private DatabusHttpClientImpl                            client                   =
                                                                                        null;
  private DatabusHttpClientImpl.Config                     clientConfigBuilder      =
                                                                                        null;
  private ConfigLoader<DatabusHttpClientImpl.StaticConfig> configLoader             =
                                                                                        null;
  private DatabusHttpClientImpl.StaticConfig               clientConfig             =
                                                                                        null;
  private int                                              id                       = -1;
  private static int                                       commonId                 = 0;
  private static final Object                              _synObj                  =
                                                                                        new Object();
  private static final List<String>                        sources                  =
                                                                                        new ArrayList<String>();
  private Properties                                       startupProps             =
                                                                                        null;
  private static String                                    relayHost                =
                                                                                        null;
  private static int                                       relayPort                = -1;
  private static String                                    sourcesString            =
                                                                                        null;
  private static final String                              _defaultPropertyFileName =
                                                                                        "databus";
  private static final String                              defaultSources           =
                                                                                        "com.linkedin.events.bizfollow.bizfollow.BizFollow";
  private static int                                       startClientPort          =
                                                                                        9000;
  private static int                                       jmxPort                  = -1;
  private static Logger                                    _logger                  =
                                                                                        null;
  private static String[]                                  additionalProperties     =
                                                                                        null;
  private static String                                    bootStrapHost            =
                                                                                        null;
  private static String                                    bootStrapPort            =
                                                                                        null;
  private static boolean                                   enableBootStrap          =
                                                                                        false;
  private boolean                                          goToBootstrap            =
                                                                                        false;
  private static String                                    checkPointDir            =
                                                                                        null;
  private static int                                       threadCount              = -1;
  private static double                                    bootStrapPercent         = -1;
  private static int                                       bootStrapThreadCount     = -1;

  static
  {
    String baseName = null;
    try
    {
      String resName = System.getProperty("databus.perf.env");

      if ((resName == null) || ("".equals(resName)))
      {
        baseName = _defaultPropertyFileName;
        System.out.println("Could not find System property named databus.perf.env. Using default property file named "
            + _defaultPropertyFileName + ".properties");
      }
      else
      {
        if (resName.endsWith(".properties"))
        {
          resName = resName.substring(0, resName.lastIndexOf("."));
        }
        baseName = resName;
      }

    }
    catch (Exception e)
    {
      baseName = _defaultPropertyFileName;
      System.out.println("Could not find System property named databus.perf.env. Using default property file named "
          + _defaultPropertyFileName + ".properties");
    }

    ResourceBundle _resource = ResourceBundle.getBundle(baseName);

    relayHost = _resource.getString("test.perf.databus.relayhost");

    try
    {
      int rPort = Integer.parseInt(_resource.getString("test.perf.databus.relayport"));
      relayPort = rPort;
    }
    catch (Exception ex)
    {

    }

    try
    {
      int rPort =
          Integer.parseInt(_resource.getString("test.perf.databus.clientportstart"));

      startClientPort = rPort;
    }
    catch (Exception ex)
    {

    }

    try
    {
      int rPort =
          Integer.parseInt(_resource.getString("test.perf.databus.clientjmxport"));
      jmxPort = rPort;
    }
    catch (Exception ex)
    {

    }

    try
    {
      String src = _resource.getString("test.perf.databus.sources");
      String[] srcs = src.split(",");
      if (srcs != null && srcs.length > 0)
      {
        for (String s : srcs)
        {
          sources.add(s.trim());
        }
      }
      else
      {
        sources.add(defaultSources);
      }
    }
    catch (Exception ex)
    {
      sources.add(defaultSources);

    }

    int cnt = 0;
    for (String s : sources)
    {
      if (cnt == 0)
      {
        sourcesString = s;
      }
      else
      {
        sourcesString = sourcesString + "," + s;
      }
      cnt++;
    }

    try
    {
      checkPointDir = _resource.getString("test.perf.databus.checkpointdir");
    }
    catch (Exception e)
    {
      checkPointDir = null;
    }

    try
    {
      bootStrapHost = _resource.getString("test.perf.databus.bootstraphost");
    }
    catch (Exception e)
    {
      bootStrapHost = null;
    }

    try
    {
      bootStrapPort = _resource.getString("test.perf.databus.bootstrapport");
    }
    catch (Exception e)
    {
      bootStrapPort = null;
    }

    if (bootStrapHost != null && bootStrapPort != null)
    {
      enableBootStrap = true;
    }
    else
    {
      enableBootStrap = false;
    }

    try
    {
      String addProps = _resource.getString("test.perf.databus.additionalparameters");
      String[] srcs = addProps.split("::");
      if (srcs != null && srcs.length > 0)
      {
        additionalProperties = new String[srcs.length];
        for (int i = 0; i < additionalProperties.length; i++)
        {
          additionalProperties[i] = srcs[i].trim();
        }
      }
    }
    catch (Exception ex)
    {

    }

    try
    {
      threadCount = Integer.parseInt(System.getProperty("test.perf.databus.threadcount"));
    }
    catch (Exception e)
    {
      e.printStackTrace();

    }

    try
    {
      bootStrapPercent =
          Double.parseDouble(System.getProperty("test.perf.databus.bootstrapthreadpercent"));
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }

    if (threadCount >= 0 && bootStrapPercent >= 0)
    {
      bootStrapThreadCount = (int) (threadCount * (bootStrapPercent / 100.0));
    }

  }

  public static void setLogger(Logger logger)
  {
    _logger = logger;
  }

  public BizFollowPerfConsumer() throws Exception
  {
    setId();

    if (bootStrapThreadCount >= 0)
    {
      if (id <= bootStrapThreadCount)
      {
        goToBootstrap = true;
      }
    }
    clientConfigBuilder = new DatabusHttpClientImpl.Config();
    clientConfigBuilder.getContainer().setIdFromName("SimpleBizfollowConsumer" + getId());
    configLoader =
        new ConfigLoader<DatabusHttpClientImpl.StaticConfig>("databus.client.",
                                                             clientConfigBuilder);

    // Populate startupProps
    if (additionalProperties != null)
    {
      startupProps = DatabusHttpClientImpl.processCommandLineArgs(additionalProperties);
    }

    if (startupProps == null)
    {
      startupProps = new Properties();
    }

    startupProps.put("databus.client.container.httpPort", ""
        + (startClientPort + getId()));
    if (jmxPort > 0)
    {
      startupProps.put("databus.client.container.jmx.jmxServicePort", jmxPort);
    }

    if (checkPointDir != null)
    {
      if (enableBootStrap && goToBootstrap)
      {
        deleteDir(checkPointDir + File.separator + id);
      }
      createDir(checkPointDir + File.separator + id);
      startupProps.put("databus.client.checkpointPersistence.fileSystem.rootDirectory",
                       checkPointDir + File.separator + id);
    }

    clientConfig = configLoader.loadConfig(startupProps);
    ServerInfoBuilder relayBuilder = clientConfig.getRuntime().getRelay("1");
    relayBuilder.setName("DefaultRelay" + getId());
    relayBuilder.setHost(relayHost);
    try
    {
      if (relayPort > 0)
      {
        relayBuilder.setPort(relayPort);
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }

    relayBuilder.setSources(sourcesString);

    if (enableBootStrap && goToBootstrap)
    {
      ServerInfoBuilder bootstrapBuilder =
          clientConfig.getRuntime().getBootstrap().getService("2");
      bootstrapBuilder.setName("DefaultBootstrapServices");
      if (bootStrapHost != null)
      {
        bootstrapBuilder.setHost(bootStrapHost);
      }
      if (bootStrapPort != null)
      {
        bootstrapBuilder.setPort(Integer.parseInt(bootStrapPort));
      }
      bootstrapBuilder.setSources(sourcesString.toString());
      clientConfig.getRuntime().getBootstrap().setEnabled(true);
    }

    client = new DatabusHttpClientImpl(clientConfig);

    // Now register consumer here
    PerfConsumer dbusProfileConsumer = new PerfConsumer();
    dbusProfileConsumer.setId("Consumer" + getId());
    client.registerDatabusStreamListener(dbusProfileConsumer, sources, null);

    if (enableBootStrap && goToBootstrap)
    {
      client.registerDatabusBootstrapListener(dbusProfileConsumer, sources, null);
    }

    // add pause processor
    try
    {
      client.getProcessorRegistry()
            .register(ConsumerPauseRequestProcessor.COMMAND_NAME,
                      new ConsumerPauseRequestProcessor(null, dbusProfileConsumer));
    }
    catch (ProcessorRegistrationConflictException e)
    {
      e.printStackTrace();
      System.out.println("Failed to register "
          + ConsumerPauseRequestProcessor.COMMAND_NAME);
    }
  }

  private static void deleteDir(String str)
  {
    try
    {
      File f = new File(str);
      boolean del = true;

      if (str != null)
      {
        String s = str.trim();
        if (s.equalsIgnoreCase("/") || "".equalsIgnoreCase(s))
        {
          del = false;
        }
      }
      else
      {
        del = false;
      }

      if (del && f.exists())
      {
        Runtime runtime = Runtime.getRuntime();
        runtime.exec("rm -rf " + f.getAbsolutePath());
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  private static void createDir(String str)
  {
    try
    {
      File f = new File(str);
      if (!f.exists())
      {
        if (!f.mkdirs())
        {
          if (null != _logger) _logger.log(Level.WARNING, "unable to create dir: " + f);
        }
      }
    }
    catch (Exception e)
    {
      _logger.log(Level.SEVERE, "createDir error: " + e);
    }
  }

  private void setId()
  {
    synchronized (_synObj)
    {
      commonId++;
      id = commonId;
    }
  }

  private int getId()
  {
    return id;
  }

  public boolean subTransaction1() throws Exception
  {
    if (_logger != null)
    {
      _logger.info("Starting Consumer with Id: " + getId());
    }
    boolean result = true;
    client.startAndBlock();
    return result;
  }

}
