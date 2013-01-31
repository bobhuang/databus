package com.linkedin.databus3.espresso.schema;
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


import com.linkedin.databus.core.DbusEventInternalWritable;
import java.util.HashMap;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus3.espresso.rpldbusproto.SendEventsRequest;
import com.linkedin.databus3.espresso.rpldbusproto.StartSendEventsRequest;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventV1;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBuffer.DbusEventIterator;
import com.linkedin.databus.core.DbusEventBufferMult;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus.core.util.PhysicalSourceConfigBuilder;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.DummyPipelineFactory;
import com.linkedin.databus2.core.container.request.BinaryProtocol;
import com.linkedin.databus2.core.container.request.CommandsRegistry;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriterStaticConfig;
import com.linkedin.databus2.core.seq.MultiServerSequenceNumberHandler;
import com.linkedin.databus2.core.seq.SequenceNumberHandlerFactory;
import com.linkedin.databus2.core.seq.ServerName;
import com.linkedin.databus2.relay.config.LogicalSourceStaticConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.SchemaRegistryStaticConfig;
import com.linkedin.databus2.schemas.SourceIdNameRegistry;
import com.linkedin.databus2.schemas.VersionedSchemaSetBackedRegistryService;
import com.linkedin.databus2.schemas.utils.Utils;
import com.linkedin.databus2.test.ConditionCheck;
import com.linkedin.databus2.test.TestUtil;
import com.linkedin.databus2.test.container.SimpleTestClientConnection;
import com.linkedin.databus2.test.container.SimpleTestServerConnection;
import com.linkedin.espresso.common.config.InvalidConfigException;
import com.linkedin.espresso.schema.SchemaRegistry;

@Test(singleThreaded=true)
public class TestEspressoSendEventsExecHandler
{
  public static final Logger LOG = Logger.getLogger(TestEspressoSendEventsExecHandler.class);

  SchemaRegistryService _schemaRegistry;
  PhysicalSourceStaticConfig[] _pConfigs;
  DbusEventBuffer.StaticConfig _defaultBufferConf;
  private final String SCHEMA_ROOTDIR_PROP_NAME = "espresso.schema.rootdir";
  private final String DEFAULT_ESPRESSO_SCHEMA_ROOTDIR = "../databus3-relay-cmdline-pkg/espresso_schemas_registry";
  private String SCHEMA_ROOTDIR;

  static
  {
    PatternLayout defaultLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c} %m%n");
    ConsoleAppender defaultAppender = new ConsoleAppender(defaultLayout);

    Logger.getRootLogger().removeAllAppenders();
    Logger.getRootLogger().addAppender(defaultAppender);

    Logger.getRootLogger().setLevel(Level.OFF);
    //Logger.getRootLogger().setLevel(Level.ERROR);
    //Logger.getRootLogger().setLevel(Level.INFO);
    //Logger.getRootLogger().setLevel(Level.DEBUG);
  }

  @BeforeClass
  public void setUp() throws Exception
  {
    DbusEventV1.byteOrder = BinaryProtocol.BYTE_ORDER;

    SCHEMA_ROOTDIR = System.getProperty(SCHEMA_ROOTDIR_PROP_NAME, DEFAULT_ESPRESSO_SCHEMA_ROOTDIR);
    LOG.info("Using espresso schema registry root: " + SCHEMA_ROOTDIR);

    createDefaultConfig();
    createPhysicalConfigs();
    createRegistry();
  }

  private void createDefaultConfig() throws com.linkedin.databus.core.util.InvalidConfigException {
    //default buffer configuration
    DbusEventBuffer.Config bufferConfigBuilder = new DbusEventBuffer.Config();
    bufferConfigBuilder.setMaxSize(1000000);
    bufferConfigBuilder.setReadBufferSize(10000);
    bufferConfigBuilder.setScnIndexSize(10000);
    _defaultBufferConf = bufferConfigBuilder.build();
  }

  private void createPhysicalConfigs() throws
  com.linkedin.databus.core.util.InvalidConfigException  {
    String dbRelayConfigFileTest = "../config/espresso_physical_sources.json";
    String dbRelayConfigFileTest1 = "../config/espresso_physical_sources1.json";


    String[] sources = new String[]{dbRelayConfigFileTest,dbRelayConfigFileTest1};
    PhysicalSourceConfigBuilder builder = new PhysicalSourceConfigBuilder(SCHEMA_ROOTDIR, sources);

    _pConfigs = builder.build();
  }

  private void createRegistry() throws
  InvalidConfigException, InvalidConfigException, DatabusException {
    SchemaRegistry.Config configBuilder;
    SchemaRegistry.StaticConfig config;

    configBuilder = new SchemaRegistry.Config();
    configBuilder.setMode("file");

    configBuilder.setRootSchemaNamespace(SCHEMA_ROOTDIR);

    config = configBuilder.build();
    SourceIdNameRegistry sourceIdNameRegistry = new SourceIdNameRegistry();

    _schemaRegistry = new EspressoBackedSchemaRegistryService(config, sourceIdNameRegistry);
  }

  private DbusEventBufferMult createEventBufferMult() throws Exception
  {
    DbusEventBufferMult bufMult = new DbusEventBufferMult();

    for (PhysicalSourceStaticConfig psourceConfig: _pConfigs) {
      DbusEventBuffer buf = new DbusEventBuffer(_defaultBufferConf, psourceConfig.getPhysicalPartition());
      bufMult.addBuffer(psourceConfig, buf);
    }

    return bufMult;
  }

  public HttpRelay.Config createRelayConfigBuilder() throws Exception
  {
    HttpRelay.Config relayConfigBuilder = new HttpRelay.Config();

    relayConfigBuilder.getEventBuffer().setMaxSize(_defaultBufferConf.getMaxSize());
    relayConfigBuilder.getEventBuffer().setReadBufferSize(_defaultBufferConf.getReadBufferSize());
    relayConfigBuilder.getEventBuffer().setScnIndexSize(_defaultBufferConf.getScnIndexSize());

    relayConfigBuilder.getSchemaRegistry().setType(SchemaRegistryStaticConfig.RegistryType.EXISTING.toString());
    relayConfigBuilder.getSchemaRegistry().useExistingService(
        new VersionedSchemaSetBackedRegistryService());


    for(PhysicalSourceStaticConfig pCfg : _pConfigs) {
      for(LogicalSourceStaticConfig lSC : pCfg.getSources()) {
        relayConfigBuilder.setSourceName("" + lSC.getId(), lSC.getName());
      }
    }

    relayConfigBuilder.getDataSources().getSequenceNumbersHandler()
                      .setType(MaxSCNReaderWriterStaticConfig.Type.IN_MEMORY.toString());

    return relayConfigBuilder;
  }


  @Test
  public void testSendSingleEventHappyPathV3() throws Exception
  {
    DbusEventBufferMult eventBufMult = createEventBufferMult();

    HttpRelay.Config relayConfigBuilder = createRelayConfigBuilder();

    HttpRelay.StaticConfig relayConfig = relayConfigBuilder.build();
    SequenceNumberHandlerFactory handlerFactory =
        relayConfig.getDataSources().getSequenceNumbersHandler().createFactory();
    MultiServerSequenceNumberHandler multiServerSeqHandler =
        new MultiServerSequenceNumberHandler(handlerFactory);

    multiServerSeqHandler.writeLastSequenceNumber(new ServerName(10), 666L);

    SourceIdNameRegistry sourcesReg = SourceIdNameRegistry.createFromIdNamePairs(relayConfig.getSourceIds());

    SendEventsRequest.ExecHandlerFactory sendEventsExecFactory =
        new SendEventsRequest.ExecHandlerFactory(eventBufMult, relayConfig,
                                                 multiServerSeqHandler,
                                                 sourcesReg, null, null, _schemaRegistry, null);
    CommandsRegistry cmdsRegistry = new CommandsRegistry();
    cmdsRegistry.registerCommand(null, StartSendEventsRequest.OPCODE,
                                 new StartSendEventsRequest.BinaryParserFactory(),
                                 sendEventsExecFactory);
    cmdsRegistry.registerCommand(null, SendEventsRequest.OPCODE,
                                 new SendEventsRequest.BinaryParserFactory(),
                                 sendEventsExecFactory);

    SimpleTestServerConnection srvConn = new SimpleTestServerConnection(DbusEventV1.byteOrder);
    srvConn.setPipelineFactory(new DummyPipelineFactory.DummyServerPipelineFactory(cmdsRegistry));
    boolean serverStarted = srvConn.startSynchronously(102, 100);
    Assert.assertTrue(serverStarted, "server started");

    SimpleTestClientConnection clientConn = new SimpleTestClientConnection(DbusEventV1.byteOrder);
    clientConn.setPipelineFactory(new DummyPipelineFactory.DummyClientPipelineFactory());
    boolean clientConnected = clientConn.startSynchronously(102, 100);
    Assert.assertTrue(clientConnected, "client connected");

    Channel clientChannel = clientConn.getChannel();

    //sleep to let connection happen
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    //Send GetLastSequence([101])

    HashMap<Integer, LogicalSource> origSources = new HashMap<Integer, LogicalSource>();
    String lSourceName=null;
    for (IdNamePair pair: relayConfig.getSourceIds())
    {
      LogicalSource newSrc = new LogicalSource((pair.getId().intValue() - 100), pair.getName());
      origSources.put(newSrc.getId(), newSrc);
      if(lSourceName == null)
      	lSourceName = newSrc.getName();
    }

    StartSendEventsRequest req1 = StartSendEventsRequest.createV3(10, origSources);
    final ChannelFuture req1WriteFuture = req1.writeToChannelAsBinary(clientChannel);

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return req1WriteFuture.isDone();}
    }, "write completed", 1000, null);

    Assert.assertTrue(req1WriteFuture.isSuccess(), "write successful");

    //check the response
    ChannelPipeline clientPipe = clientChannel.getPipeline();
    DummyPipelineFactory.SimpleResponseBytesAggregatorHandler respAggregator =
        (DummyPipelineFactory.SimpleResponseBytesAggregatorHandler)clientPipe.get(
            DummyPipelineFactory.RESPONSE_AGGREGATOR_NAME);

    //verify that we get valid response
    Assert.assertTrue(null != respAggregator, "has response aggregator");
    ChannelBuffer response1 = respAggregator.getBuffer();
    Assert.assertEquals(response1.readableBytes(), 1 + 4 + 8 + 4, "correct response size");
    byte resultCode = response1.readByte();
    Assert.assertEquals(resultCode, (byte)0);
    int responseSize = response1.readInt();
    Assert.assertEquals(responseSize, 8 + 4);
    long binlogOfs = response1.readLong();
    Assert.assertEquals(binlogOfs, 666L);
    int maxTransSize = response1.readInt();
    Assert.assertEquals(maxTransSize, _defaultBufferConf.getReadBufferSize());


    //send SendEvents command
    respAggregator.clear();

    //First generate some events
    PhysicalPartition pPartition = _pConfigs[0].getPhysicalPartition();//new PhysicalPartition(1,"name");
    DbusEventBuffer srcBuffer = new DbusEventBuffer(_defaultBufferConf, pPartition);

    srcBuffer.start(1);
    srcBuffer.startEvents();
    byte[] schemaId1 = new byte[16];
    schemaId1[0] = 1;
    schemaId1[1] = 0;

    srcBuffer.appendEvent(new DbusEventKey(1), (short) 76, (short)76, System.currentTimeMillis() * 1000000,
                          (short)2, schemaId1, new byte[100], false, null);
    srcBuffer.appendEvent(new DbusEventKey(1), (short) 76, (short)76, System.currentTimeMillis() * 1000000,
                         (short)2, schemaId1, new byte[100], false, null);
    srcBuffer.appendEvent(new DbusEventKey(1), (short) 76, (short)76, System.currentTimeMillis() * 1000000,
                         (short)2, schemaId1, new byte[100], false, null);
    srcBuffer.endEvents(123L,  null);

    DbusEventIterator eventIter = srcBuffer.acquireIterator("srcIterator");

    //skip over the first initialization event
    Assert.assertTrue(eventIter.hasNext(), "iterator has event");
    eventIter.next();

    //send first event
    Assert.assertTrue(eventIter.hasNext(), "iterator has event");
    DbusEventInternalWritable evt = eventIter.next();

    int evtSize = evt.size();
    SendEventsRequest sendEvents1 =
        SendEventsRequest.createV3(667, evt.getRawBytes().order(BinaryProtocol.BYTE_ORDER));

    final ChannelFuture send1WriteFuture = sendEvents1.writeToChannelAsBinary(clientChannel);

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return send1WriteFuture.isDone();}
    }, "write completed", 1000, null);

    response1 = respAggregator.getBuffer();
    Assert.assertEquals(response1.readableBytes(), 1 + 4 + 4, "correct response size");
    resultCode = response1.readByte();
    Assert.assertEquals(resultCode, (byte)0, "no error: " + resultCode);
    responseSize = response1.readInt();
    Assert.assertEquals(responseSize, 4);
    int eventsNum = response1.readInt();
    Assert.assertEquals(eventsNum, 1);

    //compare events
    DbusEventBuffer eventBuffer = eventBufMult.getOneBuffer(pPartition);
    
    //DbusEventBuffer eventBuffer = getBufferForPartition(eventBufMult, 1, "psource");
    final DbusEventIterator resIter = eventBuffer.acquireIterator("resIterator");

    Assert.assertTrue(resIter.hasNext(), "has result event");
    DbusEvent resEvt = resIter.next();
    Assert.assertTrue(resEvt.isValid(), "valid event");
    Assert.assertEquals(resEvt.size(), evtSize);
    Assert.assertEquals(resEvt.sequence(), evt.sequence());
    Assert.assertEquals(resEvt.key(), evt.key());
    Assert.assertEquals(resEvt.srcId(), (short)102); // rewritten src id
    String schemaId = _schemaRegistry.fetchSchemaIdForSourceNameAndVersion(lSourceName, 1).toString();
    String eventSchemaId = Utils.hex(resEvt.schemaId());
    Assert.assertEquals(eventSchemaId, schemaId);


    //send the second event
    respAggregator.clear();


    srvConn.stop();
    clientConn.stop();
  }

}
