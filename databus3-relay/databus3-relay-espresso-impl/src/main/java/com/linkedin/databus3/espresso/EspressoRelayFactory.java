package com.linkedin.databus3.espresso;
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
import java.util.Arrays;

import org.apache.log4j.Logger;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.request.BinaryProtocol;
import com.linkedin.databus2.relay.AbstractRelayFactory;
import com.linkedin.databus2.relay.RelayFactory;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.SchemaRegistryStaticConfig;
import com.linkedin.databus2.schemas.SourceIdNameRegistry;
import com.linkedin.databus3.espresso.schema.EspressoBackedSchemaRegistryService;
import com.linkedin.espresso.schema.SchemaRegistry;


/** A factory for relays that talk to Espresso storage nodes */
public class EspressoRelayFactory extends AbstractRelayFactory implements RelayFactory
{
  public static final Logger LOG = Logger.getLogger(EspressoRelayFactory.class);
  private final EspressoRelay.StaticConfigBuilder _relayConfigBuilder;
  private final PhysicalSourceStaticConfig[] _startPhysicalSrcConfigs;

  public EspressoRelayFactory(EspressoRelay.StaticConfigBuilder relayConfigBuilder,
                              PhysicalSourceStaticConfig[] fallbackPhysicalSrcConfigs,
                              SourceIdNameRegistry sourcesIdNameRegistr)
  {
    super(sourcesIdNameRegistr);
    DbusEvent.byteOrder = BinaryProtocol.BYTE_ORDER;
    _relayConfigBuilder = relayConfigBuilder;
    _startPhysicalSrcConfigs = fallbackPhysicalSrcConfigs;
  }
  public EspressoRelayFactory(EspressoRelay.StaticConfigBuilder relayConfigBuilder,
                              PhysicalSourceStaticConfig[] fallbackPhysicalSrcConfigs)
  {
    this(relayConfigBuilder, fallbackPhysicalSrcConfigs, null);
  }

  @Override
  public HttpRelay createRelay() throws DatabusException
  {
    SchemaRegistry.StaticConfig espressoSchemasConf = null;
    EspressoBackedSchemaRegistryService regService = null;
    try
    {
      espressoSchemasConf = _relayConfigBuilder.getEspressoSchemas().build();
      regService = new EspressoBackedSchemaRegistryService(espressoSchemasConf,
                                                           getSourcesIdNameRegistry());

      _relayConfigBuilder.getSchemaRegistry().setType(SchemaRegistryStaticConfig.RegistryType.EXISTING.toString());
      _relayConfigBuilder.getSchemaRegistry().useExistingService(regService);
    }
    catch (com.linkedin.espresso.common.config.InvalidConfigException ice)
    {
      throw new InvalidConfigException(ice);
    }

    EspressoRelay.StaticConfig relayConfig = _relayConfigBuilder.build();
    boolean cmEnabled = relayConfig.getClusterManager().getEnabled();
    
    String[] dbNames = null;
    if (null != relayConfig.getEspressoDBs())
    {
      dbNames = relayConfig.getEspressoDBs().split(",");
      Arrays.sort(dbNames);
      for (String dbName: dbNames)
      {
        String name = dbName.trim();
        if (name.length() > 0) {
          regService.loadAllSourcesOnlyIfNewEspressoDB(name);
        }
      }
      LOG.info("starting logical source ids: " + getSourcesIdNameRegistry().getAllSources());
    }
    
    PhysicalSourceStaticConfig[] pConfigs = _startPhysicalSrcConfigs;
    EspressoRelay relay = null;
    
    if (! cmEnabled) { 
    	LOG.info("Cluster Manager Integration is bot enabled. Creating relay");
    	if (null == pConfigs) {
    		pConfigs = relayConfig.getPhysicalSourcesConfigs();     
    	}
    	relay  = createEspressoRelayObject(relayConfig, pConfigs, regService);
    } else  {
    	LOG.info("Cluster Manager Integration is enabled. Creating empty relay");
    	relay  = createEspressoRelayObject(relayConfig, new PhysicalSourceStaticConfig[0], regService);
    	LOG.info("Done with initialization");
    }

    return relay;
  }
  
  public EspressoRelay createEspressoRelayObject(EspressoRelay.StaticConfig relayConfig, PhysicalSourceStaticConfig[] pConfigs, 
		  										EspressoBackedSchemaRegistryService regService) throws InvalidConfigException, DatabusException {
	  try
	  {
		  return new EspressoRelay(relayConfig, pConfigs, getSourcesIdNameRegistry(), regService);
	  }
	  catch (IOException e)
	  {
		  throw new DatabusException("Error creating a relay", e);
	  }

  }

  public EspressoRelay.StaticConfigBuilder getRelayConfigBuilder()
  {
    return _relayConfigBuilder;
  }

}
