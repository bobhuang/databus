package com.linkedin.databus3.espresso.cmclient;

import org.apache.log4j.Logger;

public class ClusterParams {
	private String _clusterName;
    private String _zkConnectString;
	private final Logger LOG = Logger.getLogger(ClusterParams.class.getName());

	public ClusterParams(String clusterName, String zkConnectString)
    {
		_clusterName = clusterName;
		_zkConnectString = zkConnectString;
		LOG.info("Obtained ClusterParams " + _clusterName + " " + _zkConnectString);
    }
    	
    public String getClusterName() {
		return _clusterName;
	}
    
	public String getZkConnectString() {
		return _zkConnectString;
	}
}
