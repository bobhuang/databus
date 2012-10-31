package com.linkedin.databus3.espresso.cmclient;

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
