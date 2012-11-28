package com.linkedin.databus.tests.inprocess;

import java.util.HashMap;
import java.util.Map;

import com.linkedin.databus.core.DataChangeEvent;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.InternalDatabusEventsListenerAbstract;
import com.linkedin.databus2.core.filter.DbusFilter;

public class DbusSCNEventListener extends InternalDatabusEventsListenerAbstract {
	private long                _maxScnSeen;
	private DbusFilter          _filter;
	private Map<Long, Long>     _maxScnPerSourceMap;
    private final long          _skipEvents;
    private long                _numEventsSeen;

	public DbusSCNEventListener(DbusFilter filter,long skipEvents)
	{
	        _filter = filter;
	        _maxScnSeen = 0;
	        _maxScnPerSourceMap = new HashMap<Long, Long>();
	        _skipEvents = skipEvents;
	        _numEventsSeen = 0;
	}

	@Override
	public void onEvent(DataChangeEvent event, long offset, int size)
	{
        _numEventsSeen++;
        if ( _numEventsSeen <= _skipEvents )
           return;

	    if ( event instanceof DbusEvent)
	    {
	    	DbusEvent e = (DbusEvent)event;
	    	if ( (_filter.allow(e)) || e.isEndOfPeriodMarker())
	    	{
	           _maxScnSeen = Math.max(_maxScnSeen, e.sequence());
	           long srcId = e.srcId();
	           Long scn = _maxScnPerSourceMap.get(srcId);
	           long seenScn = e.sequence();
               if (null != scn)
               {
            	   if ( scn.longValue() < seenScn)
            		   _maxScnPerSourceMap.put(srcId, seenScn);
               } else {
            	   _maxScnPerSourceMap.put(srcId, seenScn);
               }
              // System.out.println("SrcId :" + srcId);
	    	}
	    }
	}

	public long getMaxScnSeen()
	{
	  return _maxScnSeen;
	}

	public long getMaxScnSeen(long srcId)
	{
		System.out.println(_maxScnPerSourceMap);
		System.out.println("SRCID is :" + srcId);
		long maxScn = _maxScnPerSourceMap.get(srcId);
		long globalScn = _maxScnPerSourceMap.get(-2);
		return Math.max(globalScn, maxScn); //Todo: Revisit. May not need perSrc Scn as globalScn is enuf
	}

	public long maxScnSeen()
	{
		return _maxScnSeen;
	}

}
