package com.linkedin.databus2.sec_consumer;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.xeril.util.Shutdownable;
import org.xeril.util.Startable;
import org.xeril.util.TimeOutException;

import com.linkedin.databus.client.DatabusClientRunnable;
import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.LastWriteTimeTrackerImpl;
import com.linkedin.databus.client.generic.DatabusFileLoggingConsumer;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.InvalidConfigException;

/**
 * A test Databus 2.0 client with a consumer that can be controlled to simulate various consumer
 * behaviors.
 */
public class SecConsumerRunnable implements Startable, Shutdownable, DatabusClientRunnable
{
  public static final String MODULE = SecConsumerRunnable.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final SecConsumerRunnable.StaticConfig _clientConfig;
  private final DatabusHttpClientImpl _httpClient;


  public SecConsumerRunnable(Properties props,LastWriteTimeTrackerImpl repTimeTracker) throws Exception
  {
    SecConsumerRunnable.Config configBuilder = new SecConsumerRunnable.Config();

    ConfigLoader<SecConsumerRunnable.StaticConfig> configLoader
        = new ConfigLoader<SecConsumerRunnable.StaticConfig>("databus.sec.consumer.",
                                                                      configBuilder);
    configLoader.loadConfig(props);

    _clientConfig = configBuilder.build();

    _httpClient = new DatabusHttpClientImpl(_clientConfig.getClient());
    _httpClient.setWriteTimeTracker(repTimeTracker);

    DatabusFileLoggingConsumer logConsumer = new DatabusFileLoggingConsumer(_clientConfig.getFileName());

    for (String[] sourcesList: _clientConfig.getSources())
    {
      if (null != sourcesList)
      {
        _httpClient.registerDatabusStreamListener(logConsumer, null,sourcesList);
        _httpClient.registerDatabusBootstrapListener(logConsumer, null, sourcesList);
      }
    }

  }
  @Override
  public DatabusHttpClientImpl getDatabusHttpClientImpl()
  {
    return _httpClient;
  }

  @Override
  public void shutdown()
  {
    LOG.info("Shutdown requested");
    _httpClient.shutdown();
  }

  @Override
  public void waitForShutdown() throws InterruptedException,
      IllegalStateException
  {
    LOG.info("Waiting for shutdown");
    _httpClient.awaitShutdown();
    LOG.info(getClass().getSimpleName()+ ": shutdown");
  }

  @Override
  public void waitForShutdown(long timeout) throws InterruptedException,
      IllegalStateException,
      TimeOutException
  {
    // TODO add timeout
    LOG.info("Waiting for shutdown");
    _httpClient.awaitShutdown();
    LOG.info(getClass().getSimpleName()+ ": shutdown");
  }

  @Override
  public void start() throws Exception
  {
    LOG.info("Starting client");
    _httpClient.start();
  }

  public static class StaticConfig
  {
    private final String[][] _sources;
    private final DatabusHttpClientImpl.StaticConfig _client;
    private final String _fileName;

    public String getFileName() {
		return _fileName;
	}

	public StaticConfig(String[][] sources, DatabusHttpClientImpl.StaticConfig client, String fileName)
    {
      super();
      _sources = sources;
      _client = client;
      _fileName = fileName;
    }

    public String[][] getSources()
    {
      return _sources;
    }

    public DatabusHttpClientImpl.StaticConfig getClient()
    {
      return _client;
    }
  }

  public static class Config implements ConfigBuilder<StaticConfig>
  {
    private final ArrayList<String> _sources;
    private DatabusHttpClientImpl.Config _client;
    private String _fileName = "databus2_events.log";

    public Config()
    {
      _sources = new ArrayList<String>(10);
      _client = new DatabusHttpClientImpl.Config();
    }

    public String getSources(int index)
    {
      return getOrAddSources(index);
    }

    public void setSources(int index, String value)
    {
      getOrAddSources(index);
      _sources.set(index, value);
    }

    @Override
    public StaticConfig build() throws InvalidConfigException
    {
      ArrayList<String[]> parsedArray = new ArrayList<String[]>(_sources.size());

      for (String sourcesList: _sources)
      {
        sourcesList = sourcesList.trim();
        if (sourcesList.isEmpty()) continue;

        String[] parsedList = sourcesList.split(",");
        for (int i = 0; i < parsedList.length; ++i)
        {
          parsedList[i] = parsedList[i].trim();
        }

        if (parsedList.length > 0) parsedArray.add(parsedList);
      }

      String[][] compactedList = new String[parsedArray.size()][];
      parsedArray.toArray(compactedList);

      return new StaticConfig(compactedList, _client.build(), _fileName);
    }

    private String getOrAddSources(int index)
    {
      for (int i = _sources.size(); i <= index; ++i)
      {
        _sources.add("");
      }

      return _sources.get(index);
    }

    public DatabusHttpClientImpl.Config getClient()
    {
      return _client;
    }

    public void setClient(DatabusHttpClientImpl.Config client)
    {
      _client = client;
    }

	public String getFileName() {
		return _fileName;
	}

	public void setFileName(String _fileName) {
		this._fileName = _fileName;
	}
  }

}
