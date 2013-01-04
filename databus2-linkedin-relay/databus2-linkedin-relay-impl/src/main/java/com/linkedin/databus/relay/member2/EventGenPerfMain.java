package com.linkedin.databus.relay.member2;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBuffer.AllocationPolicy;
import com.linkedin.databus.core.DbusEventBuffer.QueuePolicy;
import com.linkedin.databus.core.DbusEventBufferMult;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.util.DatabusEventRandomProducer;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.FileSystemSchemaRegistryService;

public class EventGenPerfMain
{

  static DbusEventBuffer.StaticConfig createBufferConfig(AllocationPolicy apolicy)
      throws InvalidConfigException
  {
    DbusEventBuffer.Config bufCfg  = new DbusEventBuffer.Config();
    bufCfg.setMaxSize(50000000);
    bufCfg.setScnIndexSize(20000);
    bufCfg.setReadBufferSize(1000);
    bufCfg.setAllocationPolicy(apolicy.toString());
    bufCfg.setQueuePolicy(QueuePolicy.OVERWRITE_ON_WRITE.toString());

    return bufCfg.build();
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception
  {
    PatternLayout defaultLayout = new PatternLayout("%d{ISO8601} +%r (%p) {%c{1}} %m%n");
    ConsoleAppender defaultAppender = new ConsoleAppender(defaultLayout);

    Logger.getRootLogger().removeAllAppenders();
    Logger.getRootLogger().addAppender(defaultAppender);

    Logger.getRootLogger().setLevel(Level.ERROR);

    DatabusEventProfileRandomProducer.LOG.setLevel(Level.INFO);
    DatabusEventRandomProducer.LOG.setLevel(Level.INFO);

    String schemaRegistryPath = System.getProperty("EventGenPerfMain.schema_registry_path", "./");
    FileSystemSchemaRegistryService.Config schemaRegCfgBuilder =
        new FileSystemSchemaRegistryService.Config();
    schemaRegCfgBuilder.setSchemaDir(schemaRegistryPath);
    schemaRegCfgBuilder.setRefreshPeriodMs(-1);
    FileSystemSchemaRegistryService schemaReg = FileSystemSchemaRegistryService.build(schemaRegCfgBuilder);

    List<IdNamePair> sources = Arrays.asList(new IdNamePair(40L, Member2RelayServer.FULLY_QUALIFIED_PROFILE_EVENT_NAME));
    PhysicalSourceConfig psourceCfgBuilder = new PhysicalSourceConfig(sources);
    PhysicalSourceStaticConfig[] psources = new PhysicalSourceStaticConfig[]{psourceCfgBuilder.build()};

    DbusEventBuffer.StaticConfig bufCfg = createBufferConfig(AllocationPolicy.HEAP_MEMORY);
    DbusEventBufferMult bufMult = new DbusEventBufferMult(psources, bufCfg);

    DbusEventsStatisticsCollector statsCollector = new DbusEventsStatisticsCollector(1, "test", true, false, null);


    int eps = 3000;
    int duration = 30000;
    int maxLen = 10000;
    int minLen = 2000;

    DatabusEventRandomProducer.Config prodCfg = new DatabusEventRandomProducer.Config();
    prodCfg.setDuration(duration);
    prodCfg.setEventRate(eps);
    prodCfg.setMaxLength(maxLen);
    prodCfg.setMinLength(minLen);
    prodCfg.setMinEventsPerWindow(1);
    prodCfg.setMaxEventsPerWindow(30);

    DatabusEventProfileRandomProducer prod =
        new DatabusEventProfileRandomProducer(bufMult, eps, duration, sources, schemaReg,
                                              prodCfg.build());

    System.out.println("start generation");
    prod.startGeneration(100, eps, duration, ((long)eps) * duration, Integer.MAX_VALUE, 1,
                         ((long)eps) * duration * 10,
                         sources, statsCollector);

    Thread.sleep(duration + 1000);
    System.out.println("stop generation");
  }

}
