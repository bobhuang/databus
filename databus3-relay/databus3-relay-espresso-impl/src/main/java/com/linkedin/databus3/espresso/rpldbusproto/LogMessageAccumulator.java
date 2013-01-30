package com.linkedin.databus3.espresso.rpldbusproto;
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



/**
 * @author: ssubrama
 *
 * This class holds the string to be logged for each event that SendEventsRequest processes.
 *
 * The idea is to clear the LogMessageAccumulator when an event is received, append to it
 * at various stages, and then log it as a trace (or debug) log at the end of processing.
 *
 * If an error or exception is detected during processing, log the current contents of the
 * LogMessageAccumulator as a part of the error/exception log so we can get information
 * about the event in the logs.
 */
public class LogMessageAccumulator
{
  private static final int DEFAULT_CAPACITY = 256;
  private StringBuilder _sb = null;

  public LogMessageAccumulator()
  {
    _sb = new StringBuilder(DEFAULT_CAPACITY);
  }

  public void append(String s)
  {
    _sb.append(s);
  }

  public String toString()
  {
    return _sb.toString();
  }

  public void reset()
  {
    _sb = new StringBuilder(DEFAULT_CAPACITY);
  }
}
