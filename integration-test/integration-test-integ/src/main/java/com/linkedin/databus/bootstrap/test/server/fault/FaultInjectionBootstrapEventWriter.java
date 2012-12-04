package com.linkedin.databus.bootstrap.test.server.fault;

import java.nio.channels.WritableByteChannel;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.linkedin.databus.bootstrap.api.BootstrapEventProcessResult;
import com.linkedin.databus.bootstrap.api.BootstrapProcessingException;
import com.linkedin.databus.bootstrap.common.BootstrapEventProcessResultImpl;
import com.linkedin.databus.bootstrap.server.BootstrapEventWriter;
import com.linkedin.databus.bootstrap.test.server.fault.BootstrapServerFault.FaultResult;
import com.linkedin.databus.core.Encoding;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.core.filter.DbusFilter;


public class FaultInjectionBootstrapEventWriter extends BootstrapEventWriter 
{
	private BootstrapServerFault _fault;
	
	public FaultInjectionBootstrapEventWriter(WritableByteChannel writeChannel,
			long clientFreeBufferSize, DbusFilter filter, Encoding enc, BootstrapServerFault fault) 
	{
		super(writeChannel, clientFreeBufferSize, filter, enc);
		_fault = fault;
	}
	  
	@Override
	public BootstrapEventProcessResult onEvent(ResultSet rs, 
											   DbusEventsStatisticsCollector statsCollector) 
			throws BootstrapProcessingException
	{
		int rid = -1;
	    try {
			rid = rs.getInt(1);
		} catch (SQLException e) {
		}
	    
	    FaultResult result = _fault.isFault(rid, FaultResult.FAULT_BY_CLOSE_RESPONSE, FaultResult.FAULT_BY_THROW);
	    
	    if (result == FaultResult.FAULT_BY_CLOSE_RESPONSE)
	    {
	    	return new BootstrapEventProcessResultImpl(getRowCount(), true, true);
	    } else if ( result == FaultResult.FAULT_BY_THROW) {
	    	throw new BootstrapProcessingException("Fault Check failed at rId :" + rid);
		}
	
		return super.onEvent(rs, statsCollector);
	}
}
