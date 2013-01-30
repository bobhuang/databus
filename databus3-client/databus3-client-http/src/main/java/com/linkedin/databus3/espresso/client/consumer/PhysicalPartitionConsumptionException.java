package com.linkedin.databus3.espresso.client.consumer;
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


import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus2.core.DatabusException;

/** Denotes an exception while processing events from a given physical partition */
public class PhysicalPartitionConsumptionException extends DatabusException
{
  private static final long serialVersionUID = 1L;
  final PhysicalPartition _partition;

  public PhysicalPartitionConsumptionException(PhysicalPartition pp, Throwable cause)
  {
    super("exception processing partition " + pp, cause);
    _partition = pp;
  }

  public PhysicalPartition getPartition()
  {
    return _partition;
  }

}
