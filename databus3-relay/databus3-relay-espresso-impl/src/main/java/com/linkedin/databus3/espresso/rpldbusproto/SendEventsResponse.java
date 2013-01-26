package com.linkedin.databus3.espresso.rpldbusproto;

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
