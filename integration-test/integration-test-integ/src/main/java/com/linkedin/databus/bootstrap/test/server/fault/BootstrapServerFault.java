package com.linkedin.databus.bootstrap.test.server.fault;

import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;

public class BootstrapServerFault 
{
	private FaultConfig _config;
	
	private int currNumRequest;
	
	private int currNumFaults;
	
	public enum FaultResult
	{
		NO_FAULT,
		FAULT_BY_CLOSE_RESPONSE,
		FAULT_BY_CLOSE_CHANNEL,
		FAULT_BY_THROW,
	};
	
	public BootstrapServerFault(FaultConfig config)
	{	
		_config = config;
		reset();
	}
	
	public void reset()
	{
		currNumRequest = 0;
		currNumFaults = 0;
	}
	
	public FaultResult isFault(int rId, FaultResult onFirstFail, FaultResult subsequentFail)
	{
		currNumRequest++;
		
		if (_config.getFaultType() == FaultType.NO_FAULT)
			return FaultResult.NO_FAULT;
		
		if ( _config.getFaultType() == FaultType.REQUEST_BASED)
		{
			if ( currNumRequest <= _config.getNumRequestBeforeFault() )
				return FaultResult.NO_FAULT;

			if ( (_config.getNumFaults() >= 0) && (currNumFaults > _config.getNumFaults()))
				return FaultResult.NO_FAULT;

			int freq = _config.getFrequency();

			if ( freq > 0)
			{
				if ( currNumRequest%freq == 0)
				{
					currNumFaults++;
				
					if ( currNumFaults == 1)
						return onFirstFail;
					
					return subsequentFail;
				}
			}
			return FaultResult.NO_FAULT;
		} else if ( _config.getFaultType() == FaultType.ROWID_BASED) {
			
			if ( _config.getRowIdThreshold() < 0)
				return FaultResult.NO_FAULT;
			
			if (rId >= _config.getRowIdThreshold())
			{
				currNumFaults++;
				
				if ( currNumFaults == 1)
					return onFirstFail;
				
				return subsequentFail;
			}
		}
		
		return FaultResult.NO_FAULT;
	}

	@Override
	public String toString() {
		return "Fault [_config=" + _config + ", currNumRequest="
				+ currNumRequest + ", currNumFaults=" + currNumFaults + "]";
	}
	
	public static class FaultConfig
	{
		public static final FaultType DEFAULT_FAULT_TYPE = FaultType.NO_FAULT;
		public static final int DEFAULT_NUM_FAULTS = 1;
		public static final int DEFAULT_FREQUENCY  = 1;
		public static final int DEFAULT_NUM_REQUEST_BEFORE_FAULT = 0;
		public static final int DEFAULT_ROWID_THRESHOLD = -1;

		private final FaultType faultType;
		
		// Start injecting faults from request numbered
		private final int numRequestBeforeFault;
				
		// -1 infinite faults
		private final int numFaults;
		
		// 1 => every request
		// n, every nth request
		private final int frequency;
		
		private final int rowIdThreshold;

		
		public FaultConfig(FaultType faultType, int numRequestBeforeFault,
				int numFaults, int frequency, int rowIdThreshold) {
			super();
			this.faultType = faultType;
			this.numRequestBeforeFault = numRequestBeforeFault;
			this.numFaults = numFaults;
			this.frequency = frequency;
			this.rowIdThreshold = rowIdThreshold;
		}

		public FaultConfig()
		{
			faultType = DEFAULT_FAULT_TYPE;
			numRequestBeforeFault = DEFAULT_NUM_REQUEST_BEFORE_FAULT;
			numFaults = DEFAULT_NUM_FAULTS;
			frequency = DEFAULT_FREQUENCY;
			rowIdThreshold = DEFAULT_ROWID_THRESHOLD;
		}
		
		public FaultType getFaultType() {
			return faultType;
		}

		public int getNumFaults() {
			return numFaults;
		}
		
		public int getFrequency() {
			return frequency;
		}

		public int getNumRequestBeforeFault() {
			return numRequestBeforeFault;
		}

		public int getRowIdThreshold() {
			return rowIdThreshold;
		}

		@Override
		public String toString() {
			return "FaultConfig [faultType=" + faultType
					+ ", numRequestBeforeFault=" + numRequestBeforeFault
					+ ", numFaults=" + numFaults + ", frequency=" + frequency
					+ ", rowIdThreshold=" + rowIdThreshold + "]";
		}		
	}
	
	public static class FaultConfigBuilder
		implements ConfigBuilder<FaultConfig>
	{
		public static final String DEFAULT_FAULT_TYPE = FaultType.NO_FAULT.toString();
		public static final int DEFAULT_NUM_FAULTS = 1;
		public static final int DEFAULT_FREQUENCY  = 1;
		public static final int DEFAULT_NUM_REQUEST_BEFORE_FAULT = 0;
		public static final int DEFAULT_ROWID_THRESHOLD = -1;

		
		private String faultType;
		
		// Start injecting faults from request numbered
		private int numRequestBeforeFault;
				
		// -1 infinite faults
		private int numFaults;
		
		// 1 => every request
		// n, every nth request
		private int frequency;

		// if currRid >= this threshold, trigger fail
		private int rowidThreshold;
		
		public FaultConfigBuilder()
		{
			faultType = DEFAULT_FAULT_TYPE;
			numRequestBeforeFault = DEFAULT_NUM_REQUEST_BEFORE_FAULT;
			numFaults = DEFAULT_NUM_FAULTS;
			frequency = DEFAULT_FREQUENCY;
			rowidThreshold = DEFAULT_ROWID_THRESHOLD;
		}
		
		public String getFaultType() {
			return faultType;
		}

		public void setFaultType(String faultType) {
			this.faultType = faultType;
		}

		public int getNumFaults() {
			return numFaults;
		}

		public void setNumFaults(int numFaults) {
			this.numFaults = numFaults;
		}

		public int getFrequency() {
			return frequency;
		}

		public void setFrequency(int frequency) {
			this.frequency = frequency;
		}

		public int getNumRequestBeforeFault() {
			return numRequestBeforeFault;
		}

		public void setNumRequestBeforeFault(int numRequestBeforeFault) {
			this.numRequestBeforeFault = numRequestBeforeFault;
		}

		@Override
		public String toString() {
			return "FaultConfig [faultType=" + faultType
					+ ", numRequestBeforeFault=" + numRequestBeforeFault
					+ ", numFaults=" + numFaults + ", frequency=" + frequency
					+ "]";
		}

		public int getRowidThreshold() {
			return rowidThreshold;
		}

		public void setRowidThreshold(int rowidThreshold) {
			this.rowidThreshold = rowidThreshold;
		}

		@Override
		public FaultConfig build() throws InvalidConfigException 
		{
			FaultType type = null;
			try
			{
				type = FaultType.valueOf(faultType);
			} catch (Exception ex) {
				throw new InvalidConfigException(ex);
			}
			return new FaultConfig(type, numRequestBeforeFault, numFaults, frequency, rowidThreshold);
		}		
	}
	
	public enum FaultType
	{
		NO_FAULT,
		REQUEST_BASED,
		ROWID_BASED,			
	};
}
