package com.linkedin.databus.relay.fault;
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


import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.container.request.ChannelCloseFaultInjectionRequestProcessor;
import com.linkedin.databus.container.request.FaultInjectionRequestProcessor;
import com.linkedin.databus.container.request.GenerateDataEventsRequestProcessor;
import com.linkedin.databus.container.request.LoadDataEventsRequestProcessor;
import com.linkedin.databus.container.request.PhysicalSourcesRequestProcessor;
import com.linkedin.databus.container.request.ReadEventsRequestProcessor;
import com.linkedin.databus.container.request.RegisterRequestProcessor;
import com.linkedin.databus.container.request.RelayStatsRequestProcessor;
import com.linkedin.databus.container.request.SourcesRequestProcessor;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.DatabusEventProducer;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.relay.liar.DatabusEventLiarRandomProducer;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.request.ConfigRequestProcessor;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.SourceIdNameRegistry;

public class FaultInjectionHttpRelay extends HttpRelay
{
    public static final String MODULE = FaultInjectionHttpRelay.class.getName();
    public static final Logger LOG = Logger.getLogger(MODULE);

	private StaticConfig _staticConfig = null;

	public FaultInjectionHttpRelay(Config config,
			PhysicalSourceStaticConfig[] pConfigs) throws IOException,
			InvalidConfigException, DatabusException {
		super(config.getHttpRelay(), pConfigs);
		_staticConfig = config.build();
		initializeFakeRelayCommandProcessors();
	}

	public FaultInjectionHttpRelay(StaticConfig config,
			PhysicalSourceStaticConfig[] pConfigs) throws IOException,
			InvalidConfigException, DatabusException {
		super(config.getHttpRelay(), pConfigs);
		_staticConfig = config;
		initializeFakeRelayCommandProcessors();
	}

	public FaultInjectionHttpRelay(StaticConfig config,
			PhysicalSourceStaticConfig[] pConfigs,
			SourceIdNameRegistry sourcesIdNameRegistry,
			SchemaRegistryService schemaRegistry) throws IOException,
			InvalidConfigException, DatabusException {
		super(config.getHttpRelay(), pConfigs, sourcesIdNameRegistry, schemaRegistry);
		_staticConfig = config;
		initializeFakeRelayCommandProcessors();
	}

    protected void initializeFakeRelayCommandProcessors() throws DatabusException
    {
		LOG.info("Static Config is " + _staticConfig);

		FaultInjectionRequestProcessor fp = new FaultInjectionRequestProcessor(null, this);

		_processorRegistry.reregister(FaultInjectionRequestProcessor.USE_FAKE_COMMAND, fp);
		_processorRegistry.reregister(FaultInjectionRequestProcessor.USE_REAL_COMMAND, fp);

		_processorRegistry.reregister(ConfigRequestProcessor.COMMAND_NAME,fp);
		fp.register(ConfigRequestProcessor.COMMAND_NAME, new ConfigRequestProcessor(null, this) , new ChannelCloseFaultInjectionRequestProcessor(null, this), _staticConfig.isCloseOnConfigRequest());

		_processorRegistry.reregister(RelayStatsRequestProcessor.COMMAND_NAME,fp);
		fp.register(RelayStatsRequestProcessor.COMMAND_NAME,new RelayStatsRequestProcessor(null, this),new ChannelCloseFaultInjectionRequestProcessor(null, this),_staticConfig.isCloseOnRelayStatsRequest());

		_processorRegistry.reregister(SourcesRequestProcessor.COMMAND_NAME,fp);
		fp.register(SourcesRequestProcessor.COMMAND_NAME,new SourcesRequestProcessor(null, this),new ChannelCloseFaultInjectionRequestProcessor(null, this),_staticConfig.isCloseOnSourcesRequest());

		_processorRegistry.reregister(RegisterRequestProcessor.COMMAND_NAME,fp);
		fp.register(RegisterRequestProcessor.COMMAND_NAME,new RegisterRequestProcessor(null, this),new ChannelCloseFaultInjectionRequestProcessor(null, this),_staticConfig.isCloseOnRegisterRequest());

		_processorRegistry.reregister(ReadEventsRequestProcessor.COMMAND_NAME,fp);
		fp.register(ReadEventsRequestProcessor.COMMAND_NAME,new ReadEventsRequestProcessor(null, this),new ChannelCloseFaultInjectionRequestProcessor(null, this),_staticConfig.isCloseOnReadEventsRequest());

		_processorRegistry.reregister(PhysicalSourcesRequestProcessor.COMMAND_NAME,fp);
		fp.register(PhysicalSourcesRequestProcessor.COMMAND_NAME,new PhysicalSourcesRequestProcessor(null, this),new ChannelCloseFaultInjectionRequestProcessor(null, this),_staticConfig.isCloseOnPhysicalSourcesRequest());

	    DatabusEventProducer liarEventProducer =
	        new DatabusEventLiarRandomProducer(getEventBuffer(), 10, 100,
	                                           getRelayStaticConfig().getSourceIds(),
	                                           getSchemaRegistryService(),
	                                           getRelayStaticConfig().getRandomProducer());

	    _processorRegistry.reregister(
	        GenerateDataEventsRequestProcessor.COMMAND_NAME,
	        new GenerateDataEventsRequestProcessor(null,
	                                               this,
	                                               liarEventProducer));
	    _processorRegistry.reregister(
	       LoadDataEventsRequestProcessor.COMMAND_NAME,
	       new LoadDataEventsRequestProcessor(getDefaultExecutorService(), this));
    }

    public static void main(String[] args) throws Exception
    {
        Properties startupProps = HttpRelay.processCommandLineArgs(args);
        Config config = new Config();

        config.getHttpRelay().getContainer().setIdFromName("LiarRelayServer.localhost");

        ConfigLoader<StaticConfig> staticConfigLoader =
            new ConfigLoader<StaticConfig>("databus.relay.", config);

        String[] liarFQN = DatabusEventLiarRandomProducer.getFullyQualifiedEventNames();
        for (int srcInd = 0; srcInd < DatabusEventLiarRandomProducer._liarSrcIdList.length; srcInd++)
        {
          config.getHttpRelay().setSourceName(String.valueOf(DatabusEventLiarRandomProducer._liarSrcIdList[srcInd]),
                               liarFQN[srcInd]);
        }

        StaticConfig staticConfig = staticConfigLoader.loadConfig(startupProps);
        LOG.info("source = " + staticConfig.getHttpRelay().getSourceIds());

        FaultInjectionHttpRelay serverContainer = new FaultInjectionHttpRelay(staticConfig, null);
        serverContainer.startAndBlock();
    }

    public static class StaticConfig
    {
    	private HttpRelay.StaticConfig httpRelay;
		private boolean closeOnSourcesRequest;
		private boolean closeOnRegisterRequest;
		private boolean closeOnReadEventsRequest;
		private boolean closeOnConfigRequest;
		private boolean closeOnRelayStatsRequest;
		private boolean closeOnPhysicalSourcesRequest;


		public StaticConfig(
				com.linkedin.databus.container.netty.HttpRelay.StaticConfig httpRelay,
				boolean closeOnSourcesRequest, boolean closeOnRegisterRequest,
				boolean closeOnReadEventsRequest,
				boolean closeOnConfigRequest, boolean closeOnRelayStatsRequest,
				boolean closeOnPhysicalSourcesRequest) {
			super();
			this.httpRelay = httpRelay;
			this.closeOnSourcesRequest = closeOnSourcesRequest;
			this.closeOnRegisterRequest = closeOnRegisterRequest;
			this.closeOnReadEventsRequest = closeOnReadEventsRequest;
			this.closeOnConfigRequest = closeOnConfigRequest;
			this.closeOnRelayStatsRequest = closeOnRelayStatsRequest;
			this.closeOnPhysicalSourcesRequest = closeOnPhysicalSourcesRequest;
		}

		public HttpRelay.StaticConfig getHttpRelay() {
			return httpRelay;
		}

		public void setHttpRelay(HttpRelay.StaticConfig httpRelay) {
			this.httpRelay = httpRelay;
		}

		public boolean isCloseOnSourcesRequest() {
			return closeOnSourcesRequest;
		}

		public void setCloseOnSourcesRequest(boolean closeOnSourcesRequest) {
			this.closeOnSourcesRequest = closeOnSourcesRequest;
		}

		public boolean isCloseOnRegisterRequest() {
			return closeOnRegisterRequest;
		}

		public void setCloseOnRegisterRequest(boolean closeOnRegisterRequest) {
			this.closeOnRegisterRequest = closeOnRegisterRequest;
		}

		public boolean isCloseOnReadEventsRequest() {
			return closeOnReadEventsRequest;
		}

		public void setCloseOnReadEventsRequest(boolean closeOnReadEventsRequest) {
			this.closeOnReadEventsRequest = closeOnReadEventsRequest;
		}

		public boolean isCloseOnConfigRequest() {
			return closeOnConfigRequest;
		}

		public void setCloseOnConfigRequest(boolean closeOnConfigRequest) {
			this.closeOnConfigRequest = closeOnConfigRequest;
		}

		public boolean isCloseOnRelayStatsRequest() {
			return closeOnRelayStatsRequest;
		}

		public void setCloseOnRelayStatsRequest(boolean closeOnRelayStatsRequest) {
			this.closeOnRelayStatsRequest = closeOnRelayStatsRequest;
		}

		public boolean isCloseOnPhysicalSourcesRequest() {
			return closeOnPhysicalSourcesRequest;
		}

		public void setCloseOnPhysicalSourcesRequest(
				boolean closeOnPhysicalSourcesRequest) {
			this.closeOnPhysicalSourcesRequest = closeOnPhysicalSourcesRequest;
		}

		@Override
		public String toString() {
			return "StaticConfig [httpRelay=" + httpRelay
					+ ", closeOnSourcesRequest=" + closeOnSourcesRequest
					+ ", closeOnRegisterRequest=" + closeOnRegisterRequest
					+ ", closeOnReadEventsRequest=" + closeOnReadEventsRequest
					+ ", closeOnConfigRequest=" + closeOnConfigRequest
					+ ", closeOnRelayStatsRequest=" + closeOnRelayStatsRequest
					+ ", closeOnPhysicalSourcesRequest="
					+ closeOnPhysicalSourcesRequest + "]";
		}



    }

	public static class Config  implements ConfigBuilder<StaticConfig>
	{
		private HttpRelay.Config httpRelay = new HttpRelay.Config();

		private boolean closeOnSourcesRequest;
		private boolean closeOnRegisterRequest;
		private boolean closeOnReadEventsRequest;
		private boolean closeOnConfigRequest;
		private boolean closeOnRelayStatsRequest;
		private boolean closeOnPhysicalSourcesRequest;

		public Config() throws IOException
		{
			super();
		}

		@Override
		public StaticConfig build() throws InvalidConfigException
		{
			return new StaticConfig(httpRelay.build(), closeOnSourcesRequest, closeOnRegisterRequest, closeOnReadEventsRequest, closeOnConfigRequest, closeOnRelayStatsRequest, closeOnPhysicalSourcesRequest);
		}

		public HttpRelay.Config getHttpRelay() {
			return httpRelay;
		}

		public void setHttpRelay(HttpRelay.Config httpRelay) {
			this.httpRelay = httpRelay;
		}

		public boolean isCloseOnSourcesRequest() {
			return closeOnSourcesRequest;
		}

		public void setCloseOnSourcesRequest(boolean closeOnSourcesRequest) {
			this.closeOnSourcesRequest = closeOnSourcesRequest;
		}

		public boolean isCloseOnRegisterRequest() {
			return closeOnRegisterRequest;
		}

		public void setCloseOnRegisterRequest(boolean closeOnRegisterRequest) {
			this.closeOnRegisterRequest = closeOnRegisterRequest;
		}

		public boolean isCloseOnReadEventsRequest() {
			return closeOnReadEventsRequest;
		}

		public void setCloseOnReadEventsRequest(boolean closeOnReadEventsRequest) {
			this.closeOnReadEventsRequest = closeOnReadEventsRequest;
		}

		public boolean isCloseOnConfigRequest() {
			return closeOnConfigRequest;
		}

		public void setCloseOnConfigRequest(boolean closeOnConfigRequest) {
			this.closeOnConfigRequest = closeOnConfigRequest;
		}

		public boolean isCloseOnRelayStatsRequest() {
			return closeOnRelayStatsRequest;
		}

		public void setCloseOnRelayStatsRequest(boolean closeOnRelayStatsRequest) {
			this.closeOnRelayStatsRequest = closeOnRelayStatsRequest;
		}

		public boolean isCloseOnPhysicalSourcesRequest() {
			return closeOnPhysicalSourcesRequest;
		}

		public void setCloseOnPhysicalSourcesRequest(
				boolean closeOnPhysicalSourcesRequest) {
			this.closeOnPhysicalSourcesRequest = closeOnPhysicalSourcesRequest;
		}

	}
}
