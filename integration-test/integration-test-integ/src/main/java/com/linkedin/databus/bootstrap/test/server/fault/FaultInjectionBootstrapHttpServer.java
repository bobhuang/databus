package com.linkedin.databus.bootstrap.test.server.fault;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import com.linkedin.databus.bootstrap.server.BootstrapHttpServer;
import com.linkedin.databus.bootstrap.server.BootstrapRequestProcessor;
import com.linkedin.databus.bootstrap.server.BootstrapServerConfig;
import com.linkedin.databus.bootstrap.server.BootstrapServerStaticConfig;
import com.linkedin.databus.bootstrap.server.StartSCNRequestProcessor;
import com.linkedin.databus.bootstrap.server.TargetSCNRequestProcessor;
import com.linkedin.databus.bootstrap.test.server.fault.BootstrapServerFault.FaultConfig;
import com.linkedin.databus.bootstrap.test.server.fault.BootstrapServerFault.FaultConfigBuilder;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.netty.ServerContainer;
import com.linkedin.databus2.core.container.request.RequestProcessorRegistry;

public class FaultInjectionBootstrapHttpServer extends BootstrapHttpServer 
{
	private StaticConfig _config;
	public FaultInjectionBootstrapHttpServer(Config config)
			throws IOException, InvalidConfigException, DatabusException 
	{
		this(config.build());
	}

	public FaultInjectionBootstrapHttpServer(StaticConfig config)
			throws IOException, InvalidConfigException, DatabusException 
	{
		super(config.getBs());
		_config = config;
		LOG.info("Starting FaultInjectionBootstrapHttpServer with config :" + config);
		initializeFaultBSCommandProcessors();
	}
	
	protected void initializeFaultBSCommandProcessors() throws DatabusException
	{
	    LOG.info("Initializing Fault Injection Bootstrap HTTP Server");
	    try{
	      RequestProcessorRegistry processorRegistry = getProcessorRegistry();
	      
	      processorRegistry.reregister(BootstrapRequestProcessor.COMMAND_NAME,
	              new FaultInjectionBootstrapRequestProcessor(null, _config.getBs(), this, _config.getBootstrapFaultConfig()));
	      processorRegistry.reregister(StartSCNRequestProcessor.COMMAND_NAME,
	              new FaultInjectionRequestProcessor(null, new StartSCNRequestProcessor(null, _config.getBs(), this), _config.getBootstrapFaultConfig()));
	      processorRegistry.reregister(TargetSCNRequestProcessor.COMMAND_NAME,
	              new FaultInjectionRequestProcessor(null, new TargetSCNRequestProcessor(null, _config.getBs(), this), _config.getBootstrapFaultConfig()));
	    }
	    catch (SQLException sqle)
	    {
	      throw new DatabusException("command registration failed", sqle);
	    }
	    catch (InstantiationException e)
	    {
	      throw new DatabusException("command registration failed", e);
	    }
	    catch (IllegalAccessException e)
	    {
	      throw new DatabusException("command registration failed", e);
	    }
	    catch (Exception e)
	    {
	      throw new DatabusException("command registration failed", e);
	    }
	    LOG.info("Done Initializing Fault Injection Bootstrap HTTP Server");

	 }  
	
	  public static void main(String[] args) throws Exception
	  {
	    // use server container to pass the command line
	    Properties startupProps = ServerContainer.processCommandLineArgs(args);


	    Config config = new Config();

	    ConfigLoader<StaticConfig> configLoader =
	      new ConfigLoader<StaticConfig>("databus.bootstrap.", config);

	    StaticConfig staticConfig = configLoader.loadConfig(startupProps);

	    FaultInjectionBootstrapHttpServer bootstrapServer = new FaultInjectionBootstrapHttpServer(staticConfig);

	    // Bind and start to accept incoming connections.
	    try
	    {
	      bootstrapServer.startAndBlock();
	    }
	    catch (Exception e)
	    {
	      LOG.error("Error starting the bootstrap server", e);
	    }
	    LOG.info("Exiting bootstrap server");
	  }

	public static class StaticConfig
	{
		private final BootstrapServerStaticConfig bs;
		
		private final FaultConfig startScnFaultConfig;
		
		private final FaultConfig targetScnFaultConfig;

		private final FaultConfig bootstrapFaultConfig;

		
		public StaticConfig(BootstrapServerStaticConfig bs,
				FaultConfig startScnFaultConfig,
				FaultConfig targetScnFaultConfig,
				FaultConfig bootstrapFaultConfig) {
			super();
			this.bs = bs;
			this.startScnFaultConfig = startScnFaultConfig;
			this.targetScnFaultConfig = targetScnFaultConfig;
			this.bootstrapFaultConfig = bootstrapFaultConfig;
		}

		public BootstrapServerStaticConfig getBs() {
			return bs;
		}

		public FaultConfig getStartScnFaultConfig() {
			return startScnFaultConfig;
		}

		public FaultConfig getTargetScnFaultConfig() {
			return targetScnFaultConfig;
		}

		public FaultConfig getBootstrapFaultConfig() {
			return bootstrapFaultConfig;
		}

		@Override
		public String toString() {
			return "StaticConfig [bs=" + bs + ", startScnFaultConfig="
					+ startScnFaultConfig + ", targetScnFaultConfig="
					+ targetScnFaultConfig + ", bootstrapFaultConfig="
					+ bootstrapFaultConfig + "]";
		}
		
	}
	
	public static class Config
		implements ConfigBuilder<StaticConfig>
	{
		private BootstrapServerConfig bs;
		
		private FaultConfigBuilder startScnFaultConfig;
		
		private FaultConfigBuilder targetScnFaultConfig;

		private FaultConfigBuilder bootstrapFaultConfig;

		public Config()
			throws Exception
		{
			bs = new BootstrapServerConfig();
			startScnFaultConfig = new FaultConfigBuilder();
			targetScnFaultConfig = new FaultConfigBuilder();
			bootstrapFaultConfig = 	new FaultConfigBuilder();
		}
		
		public BootstrapServerConfig getBs() {
			return bs;
		}

		public void setBs(BootstrapServerConfig bs) {
			this.bs = bs;
		}

		public FaultConfigBuilder getStartScnFaultConfig() {
			return startScnFaultConfig;
		}

		public void setStartScnFaultConfig(FaultConfigBuilder startScnFaultConfig) {
			this.startScnFaultConfig = startScnFaultConfig;
		}

		public FaultConfigBuilder getTargetScnFaultConfig() {
			return targetScnFaultConfig;
		}

		public void setTargetScnFaultConfig(FaultConfigBuilder targetScnFaultConfig) {
			this.targetScnFaultConfig = targetScnFaultConfig;
		}

		public FaultConfigBuilder getBootstrapFaultConfig() {
			return bootstrapFaultConfig;
		}

		public void setBootstrapFaultConfig(FaultConfigBuilder bootstrapFaultConfig) {
			this.bootstrapFaultConfig = bootstrapFaultConfig;
		}

		@Override
		public StaticConfig build() throws InvalidConfigException {
			return new StaticConfig(bs.build(),startScnFaultConfig.build(), targetScnFaultConfig.build(),bootstrapFaultConfig.build());
		}

		@Override
		public String toString() {
			return "Config [bs=" + bs + ", startScnFaultConfig="
					+ startScnFaultConfig + ", targetScnFaultConfig="
					+ targetScnFaultConfig + ", bootstrapFaultConfig="
					+ bootstrapFaultConfig + "]";
		}	
		
		
	}	
}
