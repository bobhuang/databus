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


import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import com.linkedin.databus.core.DbusEventV1;
import com.linkedin.databus2.core.container.request.SimpleDatabusResponse;

public class SendEventsResponse extends SimpleDatabusResponse
{
  public static class ResponseDecoder extends FrameDecoder
  {

    @Override
    protected Object decode(ChannelHandlerContext ctx,
                            Channel channel,
                            ChannelBuffer buffer) throws Exception
    {
      throw new InternalError("Not implemented");
    }

  }

  private final int _numReadEvents;

  public SendEventsResponse(byte protocolVersion, int numReadEvents)
  {
    super(protocolVersion);
    _numReadEvents = numReadEvents;
  }

  @Override
  public ChannelBuffer serializeToBinary()
  {
    int bufferSize = 1 + 4 + 4;
    ChannelBuffer result = ChannelBuffers.buffer(DbusEventV1.byteOrder, bufferSize);
    result.writeByte(0);
    result.writeInt(bufferSize - 1 - 4);
    result.writeInt(_numReadEvents);
    return result;
  }

  public int getNumReadEvents()
  {
    return _numReadEvents;
  }

}
