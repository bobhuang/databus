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


public interface RplDbusMonitorMBean
{
  public boolean getSlaveSQLThreadRunning() throws RplDbusException;
  public boolean getSlaveIOThreadRunning() throws RplDbusException;
  public String getMasterHost() throws RplDbusException;
  public int getMasterPort() throws RplDbusException;
  public int getMasterServerID() throws RplDbusException;
  public long getSecondsBehindMaster() throws RplDbusException;
  
}
