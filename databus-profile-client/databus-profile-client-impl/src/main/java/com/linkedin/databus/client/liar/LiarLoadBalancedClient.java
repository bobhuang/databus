package com.linkedin.databus.client.liar;
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


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.databus.client.consumer.AbstractDatabusCombinedConsumer;
import com.linkedin.databus.client.generic.ClusterFileLoggingClient;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DatabusRegistration;
import com.linkedin.databus.client.pub.DbusClusterConsumerFactory;
import com.linkedin.databus.client.pub.DbusClusterInfo;
import com.linkedin.databus.client.pub.DbusModPartitionedFilterFactory;
import com.linkedin.databus.client.pub.DbusPartitionInfo;
import com.linkedin.databus.client.pub.DbusPartitionListener;
import com.linkedin.databus.client.pub.DbusServerSideFilterFactory;
import com.linkedin.databus.client.registration.DatabusV2RegistrationImpl;
import com.linkedin.databus2.core.DatabusException;

public class LiarLoadBalancedClient 
	extends ClusterFileLoggingClient 
{
	public static final String MODULE = LiarLoadBalancedClient.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);
	public static final String LIAR_JOB_RELAY_SOURCE_NAME = "com.linkedin.events.liar.jobrelay.LiarJobRelay";
	public static final String LIAR_MEMBER_RELAY_SOURCE_NAME = "com.linkedin.events.liar.memberrelay.LiarMemberRelay";

	@Override
	protected String[] getSources() 
	{
		String[] sources = new String[2];
		sources[0] = LIAR_JOB_RELAY_SOURCE_NAME;
		sources[1] = LIAR_MEMBER_RELAY_SOURCE_NAME;
		return sources;
	}

	@Override
	protected DbusClusterConsumerFactory createConsumerFactory(final String cluster, final String filePrefix) 
	{		
		return new DbusClusterConsumerFactory() {

			@Override
			public Collection<DatabusCombinedConsumer> createPartitionedConsumers(
					DbusClusterInfo clusterInfo, DbusPartitionInfo partitionInfo) {
				String file = filePrefix + "_" + cluster + "_" + partitionInfo.getPartitionId();
				AbstractDatabusCombinedConsumer c;
				try {
					c = new DatabusLiarConsumer(file,true);
				} catch (IOException e) {
					LOG.error("Got exception while creating consumers", e);
					throw new RuntimeException(e);
				}
				List<DatabusCombinedConsumer> consumers = new ArrayList<DatabusCombinedConsumer>();
				consumers.add(c);
				return consumers;
			}
		};
	}

	@Override
	protected DbusServerSideFilterFactory createServerSideFactory(String cluster) {
		try {
			return new DbusModPartitionedFilterFactory(getSources());
		} catch (DatabusException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected DbusPartitionListener createPartitionListener(final String cluster, final String eventDumpPrefix) {
		return new DbusPartitionListener() {
			
			@Override
			public void onDropPartition(DbusPartitionInfo partitionInfo,
					DatabusRegistration reg) {
				LOG.info("Drop partition called for partition :" + partitionInfo + " for cluster :" + cluster);
			}

			@Override
			public void onAddPartition(DbusPartitionInfo partitionInfo,
					DatabusRegistration reg) {
				LOG.info("Add partition called for partition :" + partitionInfo + " for cluster :" + cluster);
				
				DatabusV2RegistrationImpl r = (DatabusV2RegistrationImpl)reg;
				
				try {
					LOG.info("LoggingConsumer Dump File :" + eventDumpPrefix + "_" + cluster + "_" + partitionInfo.getPartitionId());
					r.getLoggingConsumer().enableEventFileTrace(eventDumpPrefix + "_" + cluster + "_" + partitionInfo.getPartitionId(),true);
				} catch (IOException e) {
					LOG.error("Unable to enable event trace for logging consumer for partition " + partitionInfo + " for cluster :" + cluster, e);
				}
			}
		};
	}

	public static void main(String args[]) throws Exception
	{
		LiarLoadBalancedClient liarClient = new LiarLoadBalancedClient();
		liarClient.mainFunction(args);
	}
}
