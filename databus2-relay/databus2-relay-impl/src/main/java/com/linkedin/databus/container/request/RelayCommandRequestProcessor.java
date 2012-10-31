package com.linkedin.databus.container.request;


import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus2.core.BufferNotFoundException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.request.AbstractRequestProcesser;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.InvalidRequestParamValueException;
import com.linkedin.databus2.core.container.request.RequestProcessingException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.log4j.Logger;


public class RelayCommandRequestProcessor extends AbstractRequestProcesser
{

  public final static Logger LOG = Logger.getLogger(RelayCommandRequestProcessor.class);
  public final static String COMMAND_NAME = "relayCommand";
  public final static String SAVE_META_STATE_PARAM = "saveMetaInfo";
  public final static String SHUTDOWN_RELAY_PARAM = "shutdownRelay";
  public final static String VALIDATE_RELAY_BUFFER_PARAM = "validateRelayBuffer";
  public final static String RESET_RELAY_BUFFER_PARAM = "resetRelayBuffer";
  public final static String RUN_GC_PARAM = "runGC";
  public final static String PREV_SCN_PARAM = "prevScn";
  public final static String BINLOG_OFFSET_PARAM = "binlogOffset";
  public final static String GET_BINLOG_OFFSET_PARAM = "getLastEventBinlogOffset";
  public final static String PRINT_RELAY_INFO_PARAM = "printRelayInfo";
  private final HttpRelay _relay;

  private final ExecutorService _executorService;

  public RelayCommandRequestProcessor(ExecutorService executorService, HttpRelay relay)
  {
    _executorService = executorService;
    _relay = relay;
  }

  @Override
  public ExecutorService getExecutorService() {
    return _executorService;
  }

  @Override
  public DatabusRequest process(DatabusRequest request) throws IOException, RequestProcessingException {

    String command = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME);
    if (null == command)
    {
      throw new InvalidRequestParamValueException(COMMAND_NAME, "command", "null");
    }

    String reply = new String("Command " + command  + " completed ");
    LOG.info("got relayCommand = " + command);
    if(command.equals(SAVE_META_STATE_PARAM)) {
      _relay.saveBufferMetaInfo(true);
    } else if (command.equals(SHUTDOWN_RELAY_PARAM)) {
      _relay.shutdown();
    } else if (command.equals(VALIDATE_RELAY_BUFFER_PARAM)) {
      _relay.validateRelayBuffers();
    }else if (command.equals(RUN_GC_PARAM)) {
      Runtime rt = Runtime.getRuntime();
      long mem = rt.freeMemory();
      LOG.info("mem before gc = " + rt.freeMemory() + " out of " + rt.totalMemory());
      long time = System.currentTimeMillis();
      System.gc();
      time = System.currentTimeMillis() - time;
      mem = rt.freeMemory() - mem;
      reply = new String("GC run. Took " + time + " millsecs. Freed " + mem + " bytes out of " + rt.totalMemory());
    } else if(command.startsWith(RESET_RELAY_BUFFER_PARAM)) {
      // We expect the request to be of the format:
      //   resetRelayBuffer/<dbName>/<partitionId>?prevScn=<long>&binlogOffset=<long>
      String [] resetCommands = command.split("/");
      if(resetCommands.length != 3) {
        throw new InvalidRequestParamValueException(COMMAND_NAME, "command", command);
      }
      String dbName = resetCommands[1];
      String dbPart = resetCommands[2];
      long prevScn = request.getRequiredLongParam(PREV_SCN_PARAM);
      long binlogOffset = request.getOptionalLongParam(BINLOG_OFFSET_PARAM, 0L);
      LOG.info("reset command = " + dbName + " part =" + dbPart);
      try
      {
        _relay.resetBuffer(new PhysicalPartition(Integer.parseInt(dbPart), dbName), prevScn, binlogOffset);
      }
      catch(BufferNotFoundException e)
      {
        reply = new String("command " + command + ":" + e.getMessage());
      }
    } else if(command.startsWith(GET_BINLOG_OFFSET_PARAM)) {
      String[] getOfsArgs = command.split("/");
      if (getOfsArgs.length != 2) {
        throw new InvalidRequestParamValueException(GET_BINLOG_OFFSET_PARAM, "Server ID", "");
      }
      int serverId;
      try
      {
        serverId = Integer.parseInt(getOfsArgs[1]);
        int [] offset  = _relay.getBinlogOffset(serverId);
        if (offset.length != 2) {
          reply = "Error getting binlog offset";
        } else {
          reply = new String("RelayLastEvent(" + offset[0] + "," + offset[1] + ")");
        }
      }
      catch(NumberFormatException e)
      {
        throw new InvalidRequestParamValueException(GET_BINLOG_OFFSET_PARAM, "Server ID", getOfsArgs[1]);
      }
      catch(DatabusException e)
      {
        reply = new String("command " + command + "failed with:" + e.getMessage());
      }
    } else if (command.startsWith(PRINT_RELAY_INFO_PARAM)) {
      try
      {
        Map<String,String> infoMap = _relay.printInfo();
        reply = makeJsonResponse(infoMap, request);
      }
      catch(Exception e)
      {
        reply = new String("command " + command + " failed with:" + e.getMessage());
      }
    } else {
      // invalid command
      reply = new String("command " + command + " is invalid. Valid commands are: " +
              SAVE_META_STATE_PARAM + "|" +
              SHUTDOWN_RELAY_PARAM + "|" +
              RUN_GC_PARAM + "|" +
              RESET_RELAY_BUFFER_PARAM + "|" +
              GET_BINLOG_OFFSET_PARAM + "|" +
              PRINT_RELAY_INFO_PARAM);
    }

    byte[] responseBytes = new byte[(reply.length() + 2)];

    System.arraycopy(reply.getBytes(), 0, responseBytes, 0, reply.length());
    int idx = reply.length();
    responseBytes[idx] = (byte)'\r';
    responseBytes[idx + 1] = (byte)'\n';

    request.getResponseContent().write(ByteBuffer.wrap(responseBytes));

    return request;
  }
}
