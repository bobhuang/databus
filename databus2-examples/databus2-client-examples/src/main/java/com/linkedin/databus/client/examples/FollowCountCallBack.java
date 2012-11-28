package com.linkedin.databus.client.examples;

import java.util.HashMap;
import java.util.List;
import java.util.Timer;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.generic.GenericRecord;

import com.linkedin.databus.client.consumer.AbstractDatabusCombinedConsumer;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.core.DbusEvent;

public class FollowCountCallBack  extends AbstractDatabusCombinedConsumer {

	ConcurrentHashMap<Integer, Integer> followCount;
	Timer timerThread;
	public FollowCountCallBack() {
		followCount = new ConcurrentHashMap<Integer, Integer>();
		timerThread = new Timer();
		timerThread.scheduleAtFixedRate(new SortMap(followCount), 5000, 5000);
		
	}

	@Override
	public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder)
	{
		return processEvent(e, eventDecoder);
	}

	@Override
	public ConsumerCallbackResult onBootstrapEvent(DbusEvent e, DbusEventDecoder eventDecoder)
	{
		return processEvent(e, eventDecoder);
	}

	public ConsumerCallbackResult processEvent(DbusEvent e, DbusEventDecoder eventDecoder)
	{
		GenericRecord record = eventDecoder.getGenericRecord(e, null);
		int memberId = (Integer)record.get("memberId");
		int companyId = (Integer)record.get("companyId");
		//System.out.println("Member id: " + memberId + " Company-id: " + companyId);
		Integer temp = followCount.get(companyId);
		if(temp == null)
		{
			temp = 1; 
			followCount.put(companyId, 1);
		}
		else
		{
			temp++;  
			followCount.put(companyId, temp);
		}

		//System.out.println("Company Id: " + companyId + " Count: " + temp);
		return ConsumerCallbackResult.SUCCESS;
	}

}
