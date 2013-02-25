package com.linkedin.databus.cluster;
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


import org.apache.log4j.Logger;

import com.linkedin.helix.participant.statemachine.StateModelFactory;

public class DatabusClusterNotifierFactory extends StateModelFactory<DatabusClusterNotifierStateModel>
{
	
	private DatabusClusterNotifier _notifier;
	private static final Logger LOG = Logger.getLogger(DatabusClusterNotifierStateModel.class);

	public DatabusClusterNotifierFactory(DatabusClusterNotifier notifier)
	{
		_notifier = notifier;
	}

	@Override
	public DatabusClusterNotifierStateModel createNewStateModel(String partition)
	{
		LOG.warn("Creating a new callback object for partition=" + partition);
		return new DatabusClusterNotifierStateModel(partition,_notifier);
	}

}
