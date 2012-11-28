package com.linkedin.databus3.espresso.cmclient;

import org.apache.log4j.Logger;

import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.participant.statemachine.StateModel;

public class LeaderStandbyStateModel extends StateModel
{
    private static Logger logger = Logger.getLogger(LeaderStandbyStateModel.class);

    private volatile boolean _isLeader = false;
    
    private final ClusterParams _relayClusterParams;
    private final ClusterParams _storageClusterParams;
    
    RelayIdealStateGenerator _risg = null;
    ResourceManager _rm = null;
    final int _relayReplicationFactor;
    
    public LeaderStandbyStateModel(
             ClusterParams relayClusterParams, ClusterParams storageClusterParams, int relayReplicationFactor, ResourceManager rm)
    {
    	super();
    	_relayClusterParams = relayClusterParams;
    	_storageClusterParams = storageClusterParams;
    	_relayReplicationFactor = relayReplicationFactor;
    	_rm = rm;
    	logger.info("Constructing LeaderStandbyState Model: Replication Factor for relay set to :" + _relayReplicationFactor);
    }
    
    public void onBecomeLeaderFromStandby(Message task,
            NotificationContext context) throws Exception
    {   
        logger.info("Became leader from Standby for partition " + task.getPartitionName());
    	_isLeader = true;
    	
    	logger.info("Creating Relay Ideal State Generator");
    	_risg = new RelayIdealStateGenerator(_relayReplicationFactor, _storageClusterParams.getZkConnectString(),
                                                                     _storageClusterParams.getClusterName(), _relayClusterParams.getZkConnectString(), 
                                                                     _relayClusterParams.getClusterName(), _rm);
    	_risg.connect(true);
    	_risg.writeIdealStatesForCluster();
    	
    	return;
    }   
    
    public void onBecomeStandbyFromLeader(Message task,
            NotificationContext context) throws Exception
    {   
        logger.info("Became Standby from Leader for partition " + task.getPartitionName());
        disconnect();
    }

    public void onBecomeStandbyFromOffline(Message task,
            NotificationContext context) throws Exception
    {   
    	_isLeader = false;
        logger.info("Became Standby for partition from Offline" + task.getPartitionName());
    }

    public void onBecomeOfflineFromStandby(Message task,
            NotificationContext context) throws Exception
    {   
    	_isLeader = false;
        logger.info("Became Offline for partition " + task.getPartitionName());
    }
    
    public void onBecomeDroppedFromOffline(Message task,
    		NotificationContext context) throws Exception
    {
    	_isLeader = false;
        logger.info("Became Dropped for partition " + task.getPartitionName());    			
    }

    public void onBecomeOfflineFromDropped(Message task,
    		NotificationContext context) throws Exception
    {
    	_isLeader = false;
        logger.info("Became Offline for partition " + task.getPartitionName());    			
    }

    public boolean isElectedLeader()
    {
    	return _isLeader;
    }
    
    @Override
    public void reset() {
    	super.reset();
    	disconnect();
    }
    
    /**
     * Invoked during the following three cases
     * 1. The relay gets a become standby notification
     * 2. There is state-model reset from Helix ( typically when ZK times out )
     * 3. The relay is shutting down, and wants to close on the cluster manager
     */
    public void disconnect()
    {
    	_isLeader = false;
        if (null != _risg)
        {
        	logger.info("Disconnect clusterManager for relay and storage adapters");
        	_risg.disconnect();
        	_risg = null;
        }
    	return;
    }
}
