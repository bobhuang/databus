package com.linkedin.databus.bootstrap.test.server.fault;

import java.sql.SQLException;
import java.util.concurrent.ExecutorService;

import com.linkedin.databus.bootstrap.server.BootstrapEventWriter;
import com.linkedin.databus.bootstrap.server.BootstrapHttpServer;
import com.linkedin.databus.bootstrap.server.BootstrapRequestProcessor;
import com.linkedin.databus.bootstrap.server.BootstrapServerStaticConfig;
import com.linkedin.databus.bootstrap.test.server.fault.BootstrapServerFault.FaultConfig;
import com.linkedin.databus.core.Encoding;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.filter.DbusFilter;

public class FaultInjectionBootstrapRequestProcessor extends
		BootstrapRequestProcessor 
{

	private final BootstrapServerFault _fault;
	
	public FaultInjectionBootstrapRequestProcessor(
			ExecutorService executorService,
			BootstrapServerStaticConfig config,
			BootstrapHttpServer bootstrapServer,
			FaultConfig faultConfig) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, SQLException 
	{
		super(executorService, config, bootstrapServer);
		_fault = new BootstrapServerFault(faultConfig);
	}

	
	@Override
	protected BootstrapEventWriter createEventWriter(DatabusRequest request, long clientFreeBufferSize, DbusFilter keyFilter, Encoding enc)
	{
	  	BootstrapEventWriter writer = new FaultInjectionBootstrapEventWriter(request.getResponseContent(), clientFreeBufferSize, keyFilter, enc, _fault);
	  	return writer;
	}
}
