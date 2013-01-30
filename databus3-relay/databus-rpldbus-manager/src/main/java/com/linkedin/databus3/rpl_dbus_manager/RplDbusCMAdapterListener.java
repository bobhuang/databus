package com.linkedin.databus3.rpl_dbus_manager;
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


import java.util.List;
import java.util.Map;

import com.linkedin.databus3.cm_utils.ClusterManagerRelayCoordinates;
import com.linkedin.databus3.cm_utils.ClusterManagerResourceKey;

public interface RplDbusCMAdapterListener
{
  public void updateRecords(Map<ClusterManagerRelayCoordinates, List<ClusterManagerResourceKey>> _externalView);
  public void notifyUpdate();
}
