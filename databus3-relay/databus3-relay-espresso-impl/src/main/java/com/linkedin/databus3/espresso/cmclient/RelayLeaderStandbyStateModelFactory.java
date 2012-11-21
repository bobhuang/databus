package com.linkedin.databus3.espresso.cmclient;

import org.apache.log4j.Logger;

import com.linkedin.helix.participant.statemachine.StateModel;
import com.linkedin.helix.participant.statemachine.StateModelFactory;

public class RelayLeaderStandbyStateModelFactory extends StateModelFactory<StateModel>{

    private static Logger logger = Logger.getLogger(RelayStateModelFactory.class);
    
    private ResourceManager _rm = null;
    private final int _relayReplicationFactor;
    
    private ClusterParams _relayClusterParams, _storageClusterParams;
    private LeaderStandbyStateModel _leaderStandbyStateModel = null;
    
    public RelayLeaderStandbyStateModelFactory(ClusterParams relayClusterParams, ClusterParams storageClusterParams, 
    										   int relayReplicationFactor, ResourceManager rm) {
    	super();
    	_rm = rm;
    	_relayClusterParams = relayClusterParams;
    	_storageClusterParams = storageClusterParams;
    	_relayReplicationFactor = relayReplicationFactor;
    }
    
    @Override
	public StateModel createNewStateModel(String partition)
	{
		logger.info("Created RelayStateModel for LeaderStandby successfully" + partition );
		_leaderStandbyStateModel = new LeaderStandbyStateModel(_relayClusterParams, _storageClusterParams, _relayReplicationFactor, _rm);
		return _leaderStandbyStateModel;
	}

    public ResourceManager getResourceManager()
    {
    	return _rm;
    }
    
    public void disconnect()
    {
    	if (null != _leaderStandbyStateModel)
    	{
    		_leaderStandbyStateModel.disconnect();
    	}
    }
}
