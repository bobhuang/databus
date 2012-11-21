package com.linkedin.databus2.examples.simple_member2_client;

import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;

import com.linkedin.databus.client.consumer.AbstractDatabusCombinedConsumer;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.core.DbusEvent;

public class SimpleMemberProfileConsumer extends AbstractDatabusCombinedConsumer
{
  public static final Logger LOG = Logger.getLogger(SimpleMemberProfileConsumer.class.getName());

  @Override
  public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    return processEvent(e, eventDecoder);
  }

  @Override
  public ConsumerCallbackResult onBootstrapEvent(DbusEvent e,
                                                 DbusEventDecoder eventDecoder)
  {
    return processEvent(e, eventDecoder);
  }

  @SuppressWarnings("unchecked")
  private ConsumerCallbackResult processEvent(DbusEvent e,
                                              DbusEventDecoder eventDecoder)
  {
    GenericRecord decodedEvent = eventDecoder.getGenericRecord(e, null);
    Integer memberId = (Integer)decodedEvent.get("memberId");
    List<GenericRecord> positions = (List<GenericRecord>)decodedEvent.get("profPositions");
    List<GenericRecord> educations = (List<GenericRecord>)decodedEvent.get("profEducations");

    int posNum = (null != positions) ? positions.size() : 0;
    int eduNum = (null != educations) ? educations.size() : 0;
    LOG.info("member: id=" + memberId + " #positions=" + posNum + " #educations=" + eduNum);
    return ConsumerCallbackResult.SUCCESS;
  }

}
