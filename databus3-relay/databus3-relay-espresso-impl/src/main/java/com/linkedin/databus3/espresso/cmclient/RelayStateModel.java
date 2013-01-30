package com.linkedin.databus3.espresso.cmclient;
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

import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.participant.statemachine.StateModel;

public class RelayStateModel extends StateModel
{
    private static Logger LOG = Logger.getLogger(RelayStateModel.class);

    private String _partition = null;

    private ResourceManager _rm = null;

    public RelayStateModel(String partition, ResourceManager rm)
    {
    	super();
    	_partition = partition;
    	_rm = rm;
    	LOG.info("Constructing RelayState Model and a ResourceManager to go with it");
    }

    public void onBecomeOfflineFromOnline(Message task,
            NotificationContext context) throws Exception
    {
      LOG.info("STATE_CHANGE on->off:stateUnitKey == " + task.getPartitionName() + ";part= " + _partition);
      assert (task.getPartitionName().equals(_partition));
      _rm.removeResource(_partition);
      LOG.info("Became offline for partition " + _partition);
    }

    public void onBecomeOnlineFromOffline(Message task,
            NotificationContext context) throws Exception
    {
      LOG.info("STATE_CHANGE off->on:stateUnitKey == " + task.getPartitionName() + ";part= " + _partition);
      assert (task.getPartitionName().equals(_partition));
      _rm.addResource(_partition);
      LOG.info("Became Online for partition " + _partition);
    }

    public void onBecomeDroppedFromOffline(Message task,
            NotificationContext context) throws Exception
    {
        LOG.info("STATE_CHANGE off->dropped:stateUnitKey == " + task.getPartitionName() + ";part= " + _partition);
    }

    @Override
    public void reset() {
    	super.reset();
    }

}
