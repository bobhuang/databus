package com.linkedin.databus.container.request;
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
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;

import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.InvalidRequestParamValueException;
import com.linkedin.databus2.core.container.request.RequestProcessingException;
import com.linkedin.databus2.core.container.request.RequestProcessor;
import com.linkedin.databus3.rpl_dbus_manager.RplDbusAdapter;
import com.linkedin.databus3.rpl_dbus_manager.RplDbusException;
import com.linkedin.databus3.rpl_dbus_manager.RplDbusManager;
import com.linkedin.databus3.rpl_dbus_manager.RplDbusState;

public class RplDbusManagerAdminRequestProcessor implements RequestProcessor
{

  public static final String MODULE = RplDbusManagerAdminRequestProcessor.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  public final static String COMMAND_NAME = "rplDbusAdmin";
  public final static String RESTART_THREAD = "restartThread";
  public final static String RPLDBUS_STATES = "rplDbusStates";
  public final static String RPLDBUS_SERVER_ID_PARAM = "serverId";
  public final static String RPLDBUS_BINLOG_OFFSET_PARAM = "binlogOffset";


  private final ExecutorService _executorService;
  private final RplDbusAdapter _rplDbusAdapter;
  private final RplDbusManager _rplDbusManager;

  public RplDbusManagerAdminRequestProcessor(ExecutorService executorService,
                                 RplDbusManager mgr)
  {
    super();
    _executorService = executorService;
    _rplDbusManager = mgr;
    if(mgr == null) {
      LOG.warn("RPLDbusManager is null. Probably is disabled");
      _rplDbusAdapter = null;
      return;
    }
    _rplDbusAdapter = mgr.getRplDbusAdapter();
    if(_rplDbusAdapter == null) {
      throw new RuntimeException("cannot get rpldbusadapter");
    }
  }

  @Override
  public ExecutorService getExecutorService()
  {
    return _executorService;
  }

  @Override
  public DatabusRequest process(DatabusRequest request) throws IOException,
      RequestProcessingException
  {
    if(_rplDbusAdapter == null || _rplDbusManager == null) {
      throw new RequestProcessingException("RplDbusAdapter/Manager is null. is RplDbusManager Enabled?");
    }

    String command = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME);
    if (null == command)
    {
      throw new InvalidRequestParamValueException(COMMAND_NAME, "command", "null");
    }

    byte[] responseBytes;
    String reply = new String("Command " + command  + " completed ");
    LOG.info("got relayCommand = " + command);
    if(command.startsWith(RESTART_THREAD)) {
      int serverId = request.getRequiredIntParam(RPLDBUS_SERVER_ID_PARAM);
      long binlogOffset = request.getOptionalLongParam(RPLDBUS_BINLOG_OFFSET_PARAM, 0L);
      LOG.info("restart command : serverId = " + serverId + " offset =" + binlogOffset);
      try {
        _rplDbusManager.restartThread(serverId, binlogOffset);
      } catch (RplDbusException e) {
        throw new RequestProcessingException(e);
      }

    } else if(command.startsWith(RPLDBUS_STATES)) {
      reply = getRplDbusStates(request);
    } else {
      // invalid command
      reply = new String("command " + command + " is invalid. Valid commands are: " +
          RESTART_THREAD + "|" + RPLDBUS_STATES);
    }

    responseBytes = new byte[(reply.length() + 2)];
    System.arraycopy(reply.getBytes(), 0, responseBytes, 0, reply.length());
    int idx = reply.length();
    responseBytes[idx] = (byte)'\r';
    responseBytes[idx + 1] = (byte)'\n';

    request.getResponseContent().write(ByteBuffer.wrap(responseBytes));

    return request;
  }

  /*
   * get the states from rpldbus slave and format it into nice JSON
   */
  private String getRplDbusStates(DatabusRequest request) throws RequestProcessingException, JsonGenerationException, JsonMappingException, IOException {
    ObjectMapper mapper = new ObjectMapper();
    boolean pretty = request.getParams().getProperty("pretty") != null;
    boolean rpldbusUrl = request.getParams().getProperty("name") != null;

    // create pretty or regular writer
    ObjectWriter writer = pretty ? mapper.defaultPrettyPrintingWriter() : mapper.writer();
    StringWriter out = new StringWriter(10240);


    Map<String, RplDbusState> map;
    try
    {
      map = _rplDbusAdapter.getStates();
    }
    catch (RplDbusException e)
    {
      throw new RequestProcessingException("failed to get state from rpldbus", e);
    }
    // output is the array of all States
    Object [] obj = new Object[map.size()];
    int i = 0;
    for(Map.Entry<String, RplDbusState> entry: map.entrySet()) {
      RplDbusState state = entry.getValue();
      RplDbusStateInfo info = new RplDbusStateInfo(_rplDbusAdapter.toString(), state);
      if(rpldbusUrl)
        obj[i++] = info;
      else
        obj[i++] = state;

    }
    writer.writeValue(out,obj);

    return out.toString();
  }

  private static class RplDbusStateInfo {
    private final String _name;
    private final RplDbusState _state;
    public RplDbusStateInfo(String name, RplDbusState state) {
      _name = name;
      _state = state;
    }
    public RplDbusState getState() {
      return _state;
    }
    public String getName() {
      return _name;
    }
  }
}
