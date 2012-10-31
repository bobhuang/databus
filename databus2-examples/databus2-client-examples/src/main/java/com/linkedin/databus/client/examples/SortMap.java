package com.linkedin.databus.client.examples;

import java.util.ArrayList;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

public class SortMap extends TimerTask {
	
	ConcurrentHashMap<Integer, Integer> followCount;
	int maxKey = 0;
	int maxValue = -1;
	
	SortMap(ConcurrentHashMap<Integer, Integer> followCount)
	{
		this.followCount = followCount;
	}
	
	@Override
	public void run() {
		
		
		for( int key: followCount.keySet())
		{
			//System.out.println("The key:" + key + " value: " + followCount.get(key));
			if(maxValue < followCount.get(key))
			{
				maxKey = key;
				maxValue = followCount.get(key);
			}
		}
		
		System.out.println("Company Id: "+ maxKey + " has a follow count: " + followCount.get(maxKey));
	}

}
