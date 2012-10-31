package com.linkedin.databus.client.generic;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.InvalidRequestParamValueException;
import com.linkedin.databus2.core.container.request.RequestProcessingException;
import com.linkedin.databus2.core.container.request.RequestProcessor;

/**
 * Interfact to pause the consumer for testing purpose
 * @author dzhang
 *
 */
public class ConsumerPauseRequestProcessor implements RequestProcessor
{

  public static final String MODULE = ConsumerPauseRequestProcessor.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public static final String COMMAND_NAME = "pauseConsumer";

  private final ExecutorService _executorService;
  private final DatabusConsumerPauseInterface _pauseConsumer;

  public ConsumerPauseRequestProcessor(ExecutorService executorService,
                                            DatabusConsumerPauseInterface pauseConsumer)
  {
    super();
    _executorService = executorService;
    _pauseConsumer = pauseConsumer;
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
    String action = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME, "");
    if (action.equals("pause"))
    {
      _pauseConsumer.pause();
      request.getResponseContent().write(ByteBuffer.wrap("{\"pauseConsumer\":\"set-pause\"}".getBytes()));
    }
    else if (action.equals("resume"))
    {
      _pauseConsumer.resume();
      request.getResponseContent().write(ByteBuffer.wrap("{\"pauseConsumer\":\"set-resume\"}".getBytes()));
    }
    else
    {
      throw new InvalidRequestParamValueException(COMMAND_NAME, "request path", action);
    }
    return request;
  }

}
