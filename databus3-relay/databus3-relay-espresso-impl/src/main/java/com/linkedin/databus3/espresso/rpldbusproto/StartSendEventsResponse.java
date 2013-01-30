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


import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.linkedin.databus2.core.container.request.BinaryProtocol;
import com.linkedin.databus2.core.container.request.ErrorResponse;
import com.linkedin.databus2.core.container.request.SimpleDatabusResponse;

public class StartSendEventsResponse extends SimpleDatabusResponse
{
  public static final String MODULE = StartSendEventsResponse.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final long _binlogOffset;
  private final int _maxTransSize;
  private final int _maxEventNum;

  private StartSendEventsResponse(byte protocolVersion, int maxTransSize, int maxEventNum,
                                  long binlogOffset)
  {
    super(protocolVersion);
    _maxTransSize = maxTransSize;
    _maxEventNum = maxEventNum;
    _binlogOffset = binlogOffset;
  }

  public static StartSendEventsResponse createV1(int maxTransSize, long binlogOffset)
  {
    return new StartSendEventsResponse((byte)1, maxTransSize, 1, binlogOffset);
  }

  public static StartSendEventsResponse createV2(int maxTransSize, int maxEventNum, long binlogOffset)
  {
    return new StartSendEventsResponse((byte)2, maxTransSize, maxEventNum, binlogOffset);
  }

  public static StartSendEventsResponse createV3(int maxTransSize, long binlogOffset)
  {
    return new StartSendEventsResponse((byte)3, maxTransSize, 1, binlogOffset);
  }

  @Override
  public ChannelBuffer serializeToBinary()
  {
    byte protoVersion = getProtocolVersion();
    if (protoVersion > 3 || protoVersion <= 0)
    {
      ErrorResponse errResponse = ErrorResponse.createUnsupportedProtocolVersionResponse(protoVersion);
      return errResponse.serializeToBinary();
    }

    int bufSize = 1 + 4 + 8 + 4;
    if (protoVersion == 2) bufSize += 4;
    ChannelBuffer responseBuffer = ChannelBuffers.buffer(BinaryProtocol.BYTE_ORDER, bufSize);
    responseBuffer.writeByte(0);
    responseBuffer.writeInt(bufSize - 1 - 4);
    responseBuffer.writeLong(_binlogOffset);
    responseBuffer.writeInt(_maxTransSize);
    if (protoVersion == 2) responseBuffer.writeInt(_maxEventNum);

    return responseBuffer;
  }

  public int getMaxTransSize()
  {
    return _maxTransSize;
  }

  public int getMaxEventNum()
  {
    return _maxEventNum;
  }

  public long getBinlogOffset()
  {
    return _binlogOffset;
  }

  @Override
  public String toString()
  {
    return "{\"protocolVersion:\":" +
             _protocolVersion +
             ",\"binlogOffset\"" +
             _binlogOffset +
             ",\"maxTransSize\"" +
             _maxTransSize +
             ",\"maxEventNum\"" +
             _maxEventNum +
             "}";
  }

}
