package com.linkedin.databus.relay.member2;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import oracle.jdbc.pool.OracleDataSource;

import org.apache.log4j.Logger;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.container.request.GenerateDataEventsRequestProcessor;
import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.DatabusEventProducer;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriter;
import com.linkedin.databus2.core.seq.MultiServerSequenceNumberHandler;
import com.linkedin.databus2.core.seq.SequenceNumberHandlerFactory;
import com.linkedin.databus2.producers.ConstantPartitionFunction;
import com.linkedin.databus2.producers.EventProducer;
import com.linkedin.databus2.producers.db.EventFactory;
import com.linkedin.databus2.producers.db.MonitoredSourceInfo;
import com.linkedin.databus2.producers.db.OracleAvroGenericEventFactory;
import com.linkedin.databus2.producers.db.OracleEventProducer;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.SchemaRegistryService;

public class Member2DatabaseRelayServer extends HttpRelay
{
  public static final String MODULE = Member2RelayServer.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public Member2DatabaseRelayServer() throws IOException, InvalidConfigException, DatabusException
  {
    this(new HttpRelay.Config(), null);
  }

  public Member2DatabaseRelayServer(HttpRelay.Config config, PhysicalSourceStaticConfig [] pConfigs)
  throws IOException, InvalidConfigException, DatabusException
  {
    super(config, pConfigs);
  }

  public Member2DatabaseRelayServer(HttpRelay.StaticConfig config, PhysicalSourceStaticConfig [] pConfigs)
  throws IOException, InvalidConfigException, DatabusException
  {
    super(config, pConfigs);
  }

  public static void main(String[] args) throws Exception {
    Properties startupProps = HttpRelay.processCommandLineArgs(args);
    Config config = new Config();

    ConfigLoader<StaticConfig> staticConfigLoader =
        new ConfigLoader<StaticConfig>("databus.relay.", config);

    HttpRelay.StaticConfig staticConfig = staticConfigLoader.loadConfig(startupProps);
    LOG.info("source = " + staticConfig.getSourceIds());

    PhysicalSourceConfig physicalSourceConfig = new PhysicalSourceConfig(staticConfig.getSourceIds());
    physicalSourceConfig.setName("member2");

    PhysicalSourceStaticConfig physicalSourceStaticConfig = physicalSourceConfig.build();
    int pSourceId = physicalSourceStaticConfig.getId();


    Member2RelayServer serverContainer = new Member2RelayServer(staticConfig, null);
    // pick any id - all should be mapped to the same buffer
    //int aSourceId = staticConfig.getSourceIds().get(0).getId().intValue();
    LogicalSource lSource = new LogicalSource(staticConfig.getSourceIds().get(0));
    DbusEventBufferAppendable dbusEventBuffer = serverContainer.getEventBuffer().getDbusEventBuffer(lSource);

    OracleDataSource ds = new OracleDataSource();
    ds.setDriverType("thin");
    ds.setServerName("devdb");
    ds.setPortNumber(1521);
    ds.setDatabaseName("db");
    ds.setUser("member2");
    ds.setPassword("member2");

    // Get the schema registry service and read the schema
    SchemaRegistryService schemaRegistryService = serverContainer.getSchemaRegistryService();

    List<MonitoredSourceInfo> sources = new ArrayList<MonitoredSourceInfo>();

    String memberProfileSchema = schemaRegistryService.fetchLatestSchemaByType("com.linkedin.events.member2.profile.MemberProfile");
    EventFactory memberProfileFactory = new OracleAvroGenericEventFactory((short)2, (short)pSourceId,
                                               memberProfileSchema, new ConstantPartitionFunction());
    MonitoredSourceInfo memberProfileSourceInfo = new MonitoredSourceInfo((short)2, "com.linkedin.events.member2.profile.MemberProfile", "member2", "member_profile", memberProfileFactory, null, false);
    sources.add(memberProfileSourceInfo);
    config.setSourceName("2", "com.linkedin.events.member2.profile.MemberProfile");

    String memberAccountSchema = schemaRegistryService.fetchLatestSchemaByType("com.linkedin.events.member2.account.MemberAccount");
    EventFactory memberAccountFactory = new OracleAvroGenericEventFactory((short)3, (short)pSourceId,
                                               memberAccountSchema, new ConstantPartitionFunction());
    MonitoredSourceInfo memberAccountSourceInfo = new MonitoredSourceInfo((short)3, "com.linkedin.events.member2.account.MemberAccount", "member2", "member_account", memberAccountFactory, null, false);
    sources.add(memberAccountSourceInfo);
    config.setSourceName("3", "com.linkedin.events.member2.account.MemberAccount");

    String memberBusinessAttrSchema = schemaRegistryService.fetchLatestSchemaByType("com.linkedin.events.member2.businessattr.MemberBusinessAttr");
    EventFactory memberBusinessAttrFactory = new OracleAvroGenericEventFactory((short)4, (short)pSourceId,
                               memberBusinessAttrSchema.toString(), new ConstantPartitionFunction());
    MonitoredSourceInfo memberBusinessAttrSourceInfo = new MonitoredSourceInfo((short)4, "com.linkedin.events.member2.businessattr.MemberBusinessAttr", "member2", "member_business_attr", memberBusinessAttrFactory, null, false);
    sources.add(memberBusinessAttrSourceInfo);
    config.setSourceName("4", "com.linkedin.events.member2.businessattr.MemberBusinessAttr");

    String memberSettingSchema = schemaRegistryService.fetchLatestSchemaByType("com.linkedin.events.member2.setting.MemberSetting");
    EventFactory memberSettingFactory = new OracleAvroGenericEventFactory((short)5, (short)pSourceId,
                                    memberSettingSchema.toString(), new ConstantPartitionFunction());
    MonitoredSourceInfo memberSettingSourceInfo = new MonitoredSourceInfo((short)5, "com.linkedin.events.member2.setting.MemberSetting", "member2", "member_setting", memberSettingFactory, null, false);
    sources.add(memberSettingSourceInfo);
    config.setSourceName("5", "com.linkedin.events.member2.setting.MemberSetting");

    //MaxSCNReaderWriter _maxScnReaderWriter =  physicalSourceStaticConfig.maxScnHandler().createOrUseExisting();
    SequenceNumberHandlerFactory handlerFactory = config.getDataSources().getSequenceNumbersHandler().build().createFactory();
    MultiServerSequenceNumberHandler maxScnReaderWriters = new MultiServerSequenceNumberHandler(handlerFactory);
    MaxSCNReaderWriter _maxScnReaderWriter = maxScnReaderWriters.getOrCreateHandler(physicalSourceStaticConfig.getPhysicalPartition());

    final EventProducer eventProducer = new OracleEventProducer(sources,
                                                                ds,
                                                                dbusEventBuffer,
                                                                true,
                                                                serverContainer.getInboundEventStatisticsCollector(),
                                                                _maxScnReaderWriter,
                                                                physicalSourceStaticConfig,
                                                                ManagementFactory.getPlatformMBeanServer());

    DatabusEventProducer databusEventProducer = new DatabusEventProducer()
    {

      @Override
      public boolean checkRunning()
      {
        return eventProducer.isRunning();
      }

      @Override
      public boolean startGeneration(long startScn,
                                     int eventsPerSecond,
                                     long durationInMilliseconds,
                                     long numEventToGenerate,
                                     int percentOfBufferToGenerate,
                                     long keyMin,
                                     long keyMax,

                                     List<IdNamePair> sources,
                                     DbusEventsStatisticsCollector statsCollector)
      {
        eventProducer.start(startScn);
        return true;
      }

      @Override
      public void stopGeneration()
      {
        eventProducer.shutdown();
      }

      @Override
      public void suspendGeneration()
        {}

      @Override
      public void resumeGeneration(long numEventToGenerate, int percentOfBufferToGenerate, long keyMin, long keyMax)
        {}

    };

    new GenerateDataEventsRequestProcessor(null,
                                           serverContainer,
                                           databusEventProducer);

    serverContainer.startAndBlock();
  }
}
