package com.linkedin.databus2.test.integ;

import java.io.File;

import com.linkedin.helix.manager.zk.ZkClient;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkServer;

public class CMZooKeeperServer {

	final private ZkServer _zkServer;
	final private int _zkPort;
	final private String _clusterName;

	private ZkClient _zkClient;

	/**
	 * 
	 * @param zkPort
	 * @param clusterName
	 */
	CMZooKeeperServer(int zkPort, String clusterName) {
		_zkPort = zkPort;
	    final String dataDir = "/tmp/cmtest/dataDir";	    
	    new File(dataDir).delete();
	    final String logDir = "/tmp/cmtest/logs";   
	    _clusterName = clusterName;
	    IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace()
	    {
	      @Override
	      public void createDefaultNameSpace(org.I0Itec.zkclient.ZkClient zkClient)
	      {
	        zkClient.deleteRecursive("/"+_clusterName);
	      }
	    };
	    
	    _zkServer = new ZkServer(dataDir, logDir, defaultNameSpace, zkPort);
	}
	
	public void start() {
	    _zkServer.start();
	    _zkClient = new ZkClient("localhost:"+_zkPort);
	}
	
	public void shutdown() {
		try {
			if (_zkClient != null)
			{
				_zkClient.close();
			}
		} catch (Exception e){}
	    _zkServer.shutdown();
	}
	
	public ZkServer getZkServer() {
		return _zkServer;
	}

	public ZkClient getZkClient() {
		return _zkClient;
	}
}
