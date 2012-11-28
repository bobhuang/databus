package com.linkedin.databus.client.bizfollow;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;

import com.linkedin.databus.client.consumer.AbstractDatabusStreamConsumer;
import com.linkedin.databus.client.generic.DatabusConsumerPauseInterface;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DatabusBootstrapConsumer;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.events.bizfollow.bizfollow.BizFollow;

public class PerfConsumer extends AbstractDatabusStreamConsumer implements
    DatabusConsumerPauseInterface,
    DatabusBootstrapConsumer
{

  public final static String MODULE                  = PerfConsumer.class.getName();
  public final static Logger LOG                     = Logger.getLogger(MODULE);
  private BizFollow          bizFollowReUse          = new BizFollow();
  private BizFollow          bizFollowReUseBootStrap = new BizFollow();
  private String             id                      = null;
  private long               invalidEvent            = 0;
  private long               invalidBootStrapEvent   = 0;
  private long               eventCount              = 0;
  private long               bootstrapEventCount     = 0;
  private boolean            _isPaused;

  static
  {
    // BasicConfigurator.configure();
    // Logger.getRootLogger().setLevel(Level.INFO);
  }

  public String getId()
  {
    return id;
  }

  public void setId(String id)
  {
    this.id = id;
  }

  public PerfConsumer()
  {

  }

  @Override
  public ConsumerCallbackResult onCheckpoint(SCN checkpointScn)
  {
    LOG.info(getId() + ": startEvents: " + checkpointScn.toString());
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    eventCount++;
    boolean success = true;
    String failureMsg = null;
    String rc = "200";
    String rm = "OK";
    long timeDiff = 0;
    try
    {
      if (!e.isValid())
      {
        invalidEvent++;
        throw new RuntimeException(getId() + " got invalid event: " + invalidEvent);
      }

      timeDiff = System.currentTimeMillis() - (e.timestampInNanos() / 1000000);
      eventDecoder.getTypedValue(e, bizFollowReUse, BizFollow.class);

      try
      {
        rm = "" + e.sequence();
      }
      catch (Exception ex)
      {

      }
    }
    catch (Exception ex)
    {
      rc = "500";
      success = false;
      failureMsg = ex.getMessage();

      StringBuffer sb = new StringBuffer("");
      sb.append("<httpSample t=\"")
        .append(timeDiff)
        .append("\" lt=\"")
        .append(timeDiff)
        .append("\" ts=\"")
        .append(System.currentTimeMillis())
        .append("\" s=\"")
        .append(success)
        .append("\" lb=\"")
        .append(getId())
        .append("\" rc=\"")
        .append(rc)
        .append("\" rm=\"")
        .append(rm)
        .append("\" tn=\"")
        .append("Thread ")
        .append(getId())
        .append("\" dt=\"")
        .append("text")
        .append("\" by=\"")
        .append("0")
        .append("\">");

      LOG.info(sb.toString());
      sb = null;

      if (!success)
      {
        LOG.info("<failureMessage>" + (failureMsg == null ? "" : failureMsg.trim())
            + "</failureMessage>");
      }

      // if (eventCount % 5000 == 0)
      // {
      // LOG.info("PERIODIC EVENT SUMMARY: " + getId() + ": TOTAL EVENTS: " + eventCount
      // + ", INVALID EVENTS: " + invalidEvent);
      // }

      throw new RuntimeException(ex);

    }

    StringBuffer sb = new StringBuffer("");
    sb.append("<httpSample t=\"")
      .append(timeDiff)
      .append("\" lt=\"")
      .append(timeDiff)
      .append("\" ts=\"")
      .append(System.currentTimeMillis())
      .append("\" s=\"")
      .append(success)
      .append("\" lb=\"")
      .append(getId())
      .append("\" rc=\"")
      .append(rc)
      .append("\" rm=\"")
      .append(rm)
      .append("\" tn=\"")
      .append("Thread ")
      .append(getId())
      .append("\" dt=\"")
      .append("text")
      .append("\" by=\"")
      .append("0")
      .append("\">");

    LOG.info(sb.toString());
    sb = null;

    if (!success)
    {
      System.out.println("<failureMessage>"
          + (failureMsg == null ? "" : failureMsg.trim()) + "</failureMessage>");
      LOG.info("<failureMessage>" + (failureMsg == null ? "" : failureMsg.trim())
          + "</failureMessage>");
    }

    // if (eventCount % 5000 == 0)
    // {
    // System.out.println("PERIODIC EVENT SUMMARY: " + getId() + ": TOTAL EVENTS: "
    // + eventCount + ", INVALID EVENTS: " + invalidEvent);
    // LOG.info("PERIODIC EVENT SUMMARY: " + getId() + ": TOTAL EVENTS: " + eventCount
    // + ", INVALID EVENTS: " + invalidEvent);
    // }

    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onEndDataEventSequence(SCN endScn)
  {
    LOG.info(getId() + ": endEvents: " + endScn);
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onEndSource(String source, Schema sourceSchema)
  {
    LOG.info(getId() + ": endSource: " + source);
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onRollback(SCN startScn)
  {
    LOG.info(getId() + ": rollback: " + startScn);
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onStartDataEventSequence(SCN startScn)
  {
    LOG.info(getId() + ": startDataEventSequence: " + startScn);
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onStartSource(String source, Schema sourceSchema)
  {
    LOG.info(getId() + ": startSource:" + source);
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public void pause()
  {
    _isPaused = true;
    LOG.info("Consumer: " + getId() + " is set to pause!");

  }

  @Override
  public void resume()
  {
    _isPaused = false;
    notifyAll();
    LOG.info("Consumer: " + getId() + " is set to resume!");

  }

  @Override
  public void waitIfPaused()
  {
    while (_isPaused)
    {
      try
      {
        wait();
      }
      catch (InterruptedException e)
      {
        // resume waiting, nothing to do
      }
    }

  }

  @Override
  public ConsumerCallbackResult onEndBootstrapSequence(SCN endScn)
  {
    waitIfPaused();
    printBootstrapEventInfo(BootstrapStage.EndBootstrapSequence, endScn.toString());

    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onEndBootstrapSource(String name, Schema sourceSchema)
  {
    waitIfPaused();
    printBootstrapEventInfo(BootstrapStage.EndBootstrapSource, name);

    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onBootstrapEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    waitIfPaused();
    // LogTypedValue(e, eventDecoder);

    bootstrapEventCount++;
    boolean success = true;
    String failureMsg = null;
    String rc = "200";
    String rm = "OK";
    long timeDiff = 0;
    try
    {
      if (!e.isValid())
      {
        invalidBootStrapEvent++;
        throw new RuntimeException(getId() + " got invalid bootstrap event: "
            + invalidEvent);
      }

      timeDiff = System.currentTimeMillis() - (e.timestampInNanos() / 1000000);
      eventDecoder.getTypedValue(e, bizFollowReUseBootStrap, BizFollow.class);

      try
      {
        rm = "" + e.sequence();
      }
      catch (Exception ex)
      {

      }
    }
    catch (Exception ex)
    {
      rc = "500";
      success = false;
      failureMsg = ex.getMessage();

      StringBuffer sb = new StringBuffer("");
      sb.append("<httpSample t=\"")
        .append(timeDiff)
        .append("\" lt=\"")
        .append(timeDiff)
        .append("\" ts=\"")
        .append(System.currentTimeMillis())
        .append("\" s=\"")
        .append(success)
        .append("\" lb=\"")
        .append(getId() + "-BootStrap")
        .append("\" rc=\"")
        .append(rc)
        .append("\" rm=\"")
        .append(rm)
        .append("\" tn=\"")
        .append("Thread ")
        .append(getId() + "-BootStrap")
        .append("\" dt=\"")
        .append("text")
        .append("\" by=\"")
        .append("0")
        .append("\">");

      LOG.info(sb.toString());
      sb = null;

      if (!success)
      {
        LOG.info("<failureMessage>" + (failureMsg == null ? "" : failureMsg.trim())
            + "</failureMessage>");
      }

      // if (eventCount % 5000 == 0)
      // {
      // LOG.info("PERIODIC BOOTSTRAP EVENT SUMMARY: " + getId() + ": TOTAL EVENTS: "
      // + bootstrapEventCount + ", INVALID EVENTS: " + invalidBootStrapEvent);
      // }

      throw new RuntimeException(ex);

    }

    StringBuffer sb = new StringBuffer("");
    sb.append("<httpSample t=\"")
      .append(timeDiff)
      .append("\" lt=\"")
      .append(timeDiff)
      .append("\" ts=\"")
      .append(System.currentTimeMillis())
      .append("\" s=\"")
      .append(success)
      .append("\" lb=\"")
      .append(getId() + "-BootStrap")
      .append("\" rc=\"")
      .append(rc)
      .append("\" rm=\"")
      .append(rm)
      .append("\" tn=\"")
      .append("Thread ")
      .append(getId() + "-BootStrap")
      .append("\" dt=\"")
      .append("text")
      .append("\" by=\"")
      .append("0")
      .append("\">");

    LOG.info(sb.toString());
    sb = null;

    if (!success)
    {
      System.out.println("<failureMessage>"
          + (failureMsg == null ? "" : failureMsg.trim()) + "</failureMessage>");
      LOG.info("<failureMessage>" + (failureMsg == null ? "" : failureMsg.trim())
          + "</failureMessage>");
    }

    // if (eventCount % 5000 == 0)
    // {
    // System.out.println("PERIODIC BOOTSTRAP EVENT SUMMARY: " + getId()
    // + ": TOTAL EVENTS: " + bootstrapEventCount + ", INVALID EVENTS: "
    // + invalidBootStrapEvent);
    // LOG.info("PERIODIC BOOTSTRAP EVENT SUMMARY: " + getId() + ": TOTAL EVENTS: "
    // + bootstrapEventCount + ", INVALID EVENTS: " + invalidBootStrapEvent);
    // }
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onBootstrapCheckpoint(SCN batchCheckpointScn)
  {
    waitIfPaused();
    printBootstrapEventInfo(BootstrapStage.OnCheckpointEvent,
                            batchCheckpointScn.toString());
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onStartBootstrapSequence(SCN startScn)
  {
    waitIfPaused();
    printBootstrapEventInfo(BootstrapStage.StartBootstrapSequence, startScn.toString());

    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onStartBootstrapSource(String name, Schema sourceSchema)
  {
    waitIfPaused();
    printBootstrapEventInfo(BootstrapStage.StartBootstrapSource, name);

    return ConsumerCallbackResult.SUCCESS;
  }

  public static enum StreamStage
  {
    StartDataEventSequence,
    EndDataEventSequence,
    OnStreamEvent,
    OnCheckpointEvent,
    StartStreamSource,
    EndStreamSource,
    InvalidStage
  }

  public static enum BootstrapStage
  {
    StartBootstrapSequence,
    EndBootstrapSequence,
    OnBootstrapEvent,
    OnCheckpointEvent,
    StartBootstrapSource,
    EndBootstrapSource
  }

  protected void printBootstrapEventInfo(BootstrapStage stage, String info)
  {
    LOG.info(stage + ": " + info);
  }

  @Override
  public ConsumerCallbackResult onStartBootstrap()
  {
    LOG.info("starting bootstrap");

    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onStopBootstrap()
  {
    LOG.info("stopping bootstrap");

    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onBootstrapRollback(SCN batchCheckpointScn)
  {
    LOG.info("bootstrap rollback");

    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onBootstrapError(Throwable err)
  {
    return ConsumerCallbackResult.SUCCESS;
  }

}
