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

import com.linkedin.databus2.core.container.request.BinaryProtocol;
import com.linkedin.databus2.core.container.request.ErrorResponse;
import com.linkedin.databus2.core.container.request.SimpleDatabusResponse;

/** Contains the response to a GetLastSequence request */
public class GetLastSequenceResponse extends SimpleDatabusResponse
{
  public static class ResponseDecoderV2 extends FrameDecoder
  {
    @Override
    protected Object decode(ChannelHandlerContext ctx,
                            Channel channel,
                            ChannelBuffer buffer) throws Exception
    {
      if (buffer.readableBytes() < 8) return null;
      byte returnCode = buffer.readByte();
      if (returnCode < 0)
      {
        buffer.readerIndex(buffer.readerIndex() - 1);
        return ErrorResponse.decodeFromChannelBuffer(buffer);
      }
      return createV2FromChannelBuffer(buffer);
    }
  }

  private final long[] _lastSeqs;

  public GetLastSequenceResponse(long[] lastSeqs, byte protocolVersion)
  {
    super(protocolVersion);
    _lastSeqs = lastSeqs;
  }

  public static GetLastSequenceResponse createV2FromChannelBuffer(ChannelBuffer buffer)
  {
    /*int responseLen = */buffer.readInt();
    long binlogOfs = buffer.readLong();

    return new GetLastSequenceResponse(new long[]{binlogOfs}, (byte)2);
  }

  public long[] getLastSeqs()
  {
    return _lastSeqs;
  }

  @Override
  public ChannelBuffer serializeToBinary()
  {
    byte protoVersion = getProtocolVersion();
    if (protoVersion > 2 || protoVersion <= 0)
    {
      ErrorResponse errResponse = ErrorResponse.createUnsupportedProtocolVersionResponse(protoVersion);
      return errResponse.serializeToBinary();
    }

    ChannelBuffer responseBuffer = ChannelBuffers.buffer(BinaryProtocol.BYTE_ORDER, 1 + 4 +
                                                         _lastSeqs.length * 8);
    responseBuffer.writeByte(0);
    responseBuffer.writeInt(_lastSeqs.length * 8);
    for (long seq: _lastSeqs) responseBuffer.writeLong(seq);

    return responseBuffer;
  }
}
