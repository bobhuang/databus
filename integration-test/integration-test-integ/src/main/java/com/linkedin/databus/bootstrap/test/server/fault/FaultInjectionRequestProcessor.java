package com.linkedin.databus.bootstrap.test.server.fault;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.linkedin.databus.bootstrap.test.server.fault.BootstrapServerFault.FaultConfig;
import com.linkedin.databus.bootstrap.test.server.fault.BootstrapServerFault.FaultResult;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.RequestProcessingException;
import com.linkedin.databus2.core.container.request.RequestProcessor;

public class FaultInjectionRequestProcessor 
	implements RequestProcessor 
{

    public static final String MODULE = FaultInjectionRequestProcessor.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);
	  
	private RequestProcessor _processor;
	private ExecutorService _execService;
	private BootstrapServerFault _fault;
	
	public FaultInjectionRequestProcessor(ExecutorService executorService,
										  RequestProcessor rp,
										  FaultConfig faultConfig)
	{
		_execService = executorService;
		_processor = rp;
		_fault  = new BootstrapServerFault(faultConfig);
	}
	
	@Override
	public DatabusRequest process(DatabusRequest request) throws IOException,
			RequestProcessingException, DatabusException 
	{
		LOG.info("Current fault config: " + _fault);
		
		FaultResult result = _fault.isFault(0, FaultResult.FAULT_BY_THROW, FaultResult.FAULT_BY_THROW);
		if ( result == FaultResult.FAULT_BY_THROW )
		{
			throw new RequestProcessingException("Fault injected Error !!");
		}
		return _processor.process(request);
	}

	@Override
	public ExecutorService getExecutorService() 
	{
		return _execService;
	}

}
