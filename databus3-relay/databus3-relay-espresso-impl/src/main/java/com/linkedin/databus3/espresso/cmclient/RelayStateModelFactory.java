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


import org.apache.log4j.Logger;

import com.linkedin.helix.participant.statemachine.StateModel;
import com.linkedin.helix.participant.statemachine.StateModelFactory;

public class RelayStateModelFactory extends StateModelFactory<StateModel>{

    private static Logger logger = Logger.getLogger(RelayStateModelFactory.class);
    
    private ResourceManager _rm;
    
    /**
     * The parameters are currently unused in this model ( only useful for LeaderStandby )
     * @param relayClusterParams
     * @param storageClusterParams
     */
    public RelayStateModelFactory(ClusterParams relayClusterParams, 
                                  ClusterParams storageClusterParams,
                                  ResourceManager rm) {
    	super();
    	_rm = rm;
    }
    
    @Override
    public StateModel createNewStateModel(String partition)
    {
      logger.info("Created RelayStateModel for OnlineOffline successfully" + partition);
      return new RelayStateModel(partition, _rm);		
    }

    public ResourceManager getResourceManager()
    {
      return _rm;
    }
}
