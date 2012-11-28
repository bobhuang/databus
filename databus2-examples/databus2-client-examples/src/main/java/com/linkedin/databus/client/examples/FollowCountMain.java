package com.linkedin.databus.client.examples;

import com.linkedin.databus.client.DatabusHttpClientImpl;


public class FollowCountMain {
	
	public static void main(String[] args) throws Exception
	  {
		DatabusHttpClientImpl.Config clientConfig = new DatabusHttpClientImpl.Config();	 
	    //Try to connect to a relay on localhost
		clientConfig.getRuntime().getRelay("1").setHost("esv4-dbus2-rly-biz-vip-b.stg");
		clientConfig.getRuntime().getRelay("1").setPort(11107);
		clientConfig.getRuntime().getRelay("1").setSources("com.linkedin.events.bizfollow.bizfollow.BizFollow");
	    
		clientConfig.getRuntime().getBootstrap().setEnabled(false);
		clientConfig.getRuntime().getBootstrap().getService("1").setHost("esv4-dbus2-bs-vip-b.stg");
		clientConfig.getRuntime().getBootstrap().getService("1").setPort(11111);
		clientConfig.getRuntime().getBootstrap().getService("1").setSources("com.linkedin.events.bizfollow.bizfollow.BizFollow");
		
		clientConfig.getCheckpointPersistence().getFileSystem().setRootDirectory("/home/nsomasun/checkpoint");

		
		FollowCountCallBack callBack = new FollowCountCallBack();
		DatabusHttpClientImpl client = new DatabusHttpClientImpl(clientConfig);
		client.registerDatabusStreamListener(callBack, null, "com.linkedin.events.bizfollow.bizfollow.BizFollow");
		client.registerDatabusBootstrapListener(callBack, null, "com.linkedin.events.bizfollow.bizfollow.BizFollow");
		
		client.startAndBlock();
	  }

}
