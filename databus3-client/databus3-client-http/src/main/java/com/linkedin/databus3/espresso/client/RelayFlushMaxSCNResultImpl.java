package com.linkedin.databus3.espresso.client;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


import com.linkedin.databus.client.pub.RelayFindMaxSCNResult;
import com.linkedin.databus.client.pub.RelayFlushMaxSCNResult;
import com.linkedin.databus.client.pub.SCN;

public class RelayFlushMaxSCNResultImpl 
	implements RelayFlushMaxSCNResult 
{
	private SCN requestedMaxSCN;
	private SCN currentMaxSCN;
	private RelayFindMaxSCNResult fetchMaxSCNResult;
	private SummaryCode resultStatus;
	
	public RelayFlushMaxSCNResultImpl(RelayFindMaxSCNResult fetchMaxSCNResult)
	{
		this.fetchMaxSCNResult = fetchMaxSCNResult;
	}
		
	public RelayFlushMaxSCNResultImpl(SCN requestedMaxSCN, 
									  SCN currentMaxSCN,
									  RelayFindMaxSCNResult fetchMaxSCNResult,
									  SummaryCode resultStatus) 
	{
		super();
		this.requestedMaxSCN = requestedMaxSCN;
		this.currentMaxSCN = currentMaxSCN;
		this.fetchMaxSCNResult = fetchMaxSCNResult;
		this.resultStatus = resultStatus;
	}

	/**
	 * Used in case of returning failure from API calls
	 * @param resultStatus
	 */
	RelayFlushMaxSCNResultImpl(SummaryCode resultStatus)
	{
		super();
		this.resultStatus = resultStatus;
	}

	public void setRequestedMaxSCN(SCN requestedMaxSCN) {
		this.requestedMaxSCN = requestedMaxSCN;
	}

	public void setCurrentMaxSCN(SCN currentMaxSCN) {
		this.currentMaxSCN = currentMaxSCN;
	}

	public void setResultStatus(SummaryCode resultStatus) {
		this.resultStatus = resultStatus;
	}

	@Override
	public SCN getRequestedMaxSCN() {
		
		return requestedMaxSCN;
	}

	@Override
	public SCN getCurrentMaxSCN() {
		
		return currentMaxSCN;
	}

	@Override
	public SummaryCode getResultStatus() {
		
		return resultStatus;
	}

	@Override
	public String toString() {
		return "RelayFlushMaxSCNResultImpl [requestedMaxSCN=" + requestedMaxSCN
				+ ", currentMaxSCN=" + currentMaxSCN + ", resultStatus="
				+ resultStatus + "]";
	}

	@Override
	public RelayFindMaxSCNResult getFetchSCNResult() {
		return fetchMaxSCNResult;
	}
	
	public void getFetchSCNResult(RelayFindMaxSCNResult fetchMaxSCNResult) {
		this.fetchMaxSCNResult = fetchMaxSCNResult;
	}
}
