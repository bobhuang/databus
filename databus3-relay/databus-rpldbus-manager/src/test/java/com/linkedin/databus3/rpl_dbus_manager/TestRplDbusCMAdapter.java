package com.linkedin.databus3.rpl_dbus_manager;
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


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus3.cm_utils.ClusterManagerRelayCoordinates;
import com.linkedin.databus3.cm_utils.ClusterManagerResourceKey;
import com.linkedin.databus3.cm_utils.ClusterManagerUtilsException;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.NotificationContext.Type;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.model.ExternalView;

public class TestRplDbusCMAdapter {

  Logger LOG = Logger.getLogger(TestRplDbusCMAdapter.class.getName());

  static
  {
    //PatternLayout defaultLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c} %m%n");
    PatternLayout defaultLayout = new PatternLayout("%m%n");
    ConsoleAppender defaultAppender = new ConsoleAppender(defaultLayout);

    Logger.getRootLogger().removeAllAppenders();
    Logger.getRootLogger().addAppender(defaultAppender);

    //Logger.getRootLogger().setLevel(Level.OFF);
    //Logger.getRootLogger().setLevel(Level.ERROR);
    // Logger.getRootLogger().setLevel(Level.INFO);
    // Logger.getRootLogger().setLevel(Level.DEBUG);


    //LOG.setLevel(Level.DEBUG);
    //Logger l = Logger.getLogger(RplDbusManager.class.getName());
    //l.setLevel(Level.DEBUG);
  }

  private static String _ev1 =
      "{" +
          "  \"id\" : \"EspressoDB8\"," +
          "  \"mapFields\" : {" +
          "    \"eat1-app122.corp.linkedin.com_12918,EspressoDB8,p0_1,MASTER\" : {" +
          "      \"localhost_11110\" : \"ONLINE\"" +
          "    }," +
          "    \"eat1-app122.corp.linkedin.com_12918,EspressoDB8,p0_1,SLAVE\" : {" +
          "      \"localhost_11120\" : \"ONLINE\"" + "," +
          "      \"localhost_11110\" : \"OFFLINE\"" +
          "    }," +
          "    \"eat1-app32.corp.linkedin.com_12918,EspressoDB8,p7_1,SLAVE\" : {" +
          "      \"localhost_11110\" : \"ONLINE\"" +  "," +
          "      \"localhost_11120\" : \"ONLINE\"" +
          "    }" +
          "  }," +
          "  \"simpleFields\" : {" +
          "  }," +
          "  \"deltaList\" : [ ]," +
          "  \"listFields\" : {" +
          "  }" +
          "}" ;

  private static String _ev2 =
      "{" +
          "  \"id\" : \"BizProfile\"," +
          "  \"mapFields\" : {" +
          "    \"eat1-app122.corp.linkedin.com_12918,BizProfile,p0_1,OFFLINE\" : {" +
          "      \"localhost_11110\" : \"ONLINE\"" + "," +
          "      \"localhost_11120\" : \"ONLINE\"" +
          "    }," +
          "    \"eat1-app32.corp.linkedin.com_12918,BizProfile,p7_1,SLAVE\" : {" +
          "      \"localhost_11110\" : \"ONLINE\"" +  "," +
          "      \"localhost_11120\" : \"ONLINE\"" +
          "    }" +
          "  }," +
          "  \"simpleFields\" : {" +
          "  }," +
          "  \"deltaList\" : [ ]," +
          "  \"listFields\" : {" +
          "  }" +
          "}" ;

  private static String _ev3 = "{" +
      "  \"id\" : \"LOCALHOSTDB\"," +
      "  \"mapFields\" : {" +
      "    \"localhost_12918,BizProfile,p0_1,SLAVE\" : {" +
      "      \"localhost_11130\" : \"OFFLINE\"" + "," +
      "      \"localhost_11140\" : \"OFFLINE\"" +
      "    }," +
      "    \"localhost_12918,BizProfile,p7_1,SLAVE\" : {" +
      "      \"localhost_11130\" : \"ONLINE\"" +  "," +
      "      \"localhost_11140\" : \"ONLINE\"" +
      "    }" +
      "  }," +
      "  \"simpleFields\" : {" +
      "  }," +
      "  \"deltaList\" : [ ]," +
      "  \"listFields\" : {" +
      "  }" +
      "}" ;

  private static String [] _testEVs = new String [] {
    "", // 0th
    _ev1, // 1th...
    _ev2,
    _ev3
  };

  /*
   * verify parsing code
   */
  @Test
  public void testRplDbusCMAdapterOnExternalViewChanged() throws ClusterManagerUtilsException {

    // simulate onExternalViewChange call
    ExternalView ev = getExternalView(1);
    ExternalView ev1 = getExternalView(2);

    RplDbusCMAdapter cmAdapter = new RplDbusCMAdapter("name", "zk");
    List<ExternalView> evList = new ArrayList<ExternalView>(2);
    evList.add(ev);
    evList.add(ev1);


    NotificationContext nc = new NotificationContext(null);
    nc.setType(Type.INIT);
    cmAdapter.onExternalViewChange(evList, nc);
    Map<ClusterManagerRelayCoordinates, List<ClusterManagerResourceKey>> view = cmAdapter.getCurrentExternalViews();

    // now verify evernalViewMap
    // should be two relays:
    //  11110 => (EDB8_p0-M, EDB8_p7-S, BP_p7-S)
    //  11120 => (EDB8_p0-S, EDB8_p7-S, BP_p7-S)
    Assert.assertEquals(view.size(), 2, "2 relays");
    for(Map.Entry<ClusterManagerRelayCoordinates, List<ClusterManagerResourceKey>> e : view.entrySet()) {
      ClusterManagerRelayCoordinates relay = e.getKey();
      int port = relay.getAddress().getPort();

      // debug print
      LOG.debug("relay="+relay+":");
      for(ClusterManagerResourceKey rk : e.getValue()) {
        LOG.debug("\t" + rk);
      }
      LOG.debug("\n");


      Assert.assertTrue(port == 11120 || port == 11110);
      if(port == 11120)
        Assert.assertEquals(e.getValue().size(), 3);  // three partitions for this relay
      if(port == 11110)
        Assert.assertEquals(e.getValue().size(), 3);  // four partitions for this relay, because we count master and slave separately
    }

    // different set
    evList = new ArrayList<ExternalView>(1);
    evList.add(ev1);

    cmAdapter = new RplDbusCMAdapter("name", "zk");
    cmAdapter.onExternalViewChange(evList, null);
    Map<ClusterManagerRelayCoordinates, List<ClusterManagerResourceKey>> view1 = cmAdapter.getCurrentExternalViews();
    // now verify evernalViewMap
    // should be two relays:
    //  11110 => (p0-M, p7-S)
    //  11120 => (p0-S, p7-S)
    Assert.assertEquals(view.size(), 2, "2 relays");
    for(Map.Entry<ClusterManagerRelayCoordinates, List<ClusterManagerResourceKey>> e : view1.entrySet()) {
      ClusterManagerRelayCoordinates relay = e.getKey();
      int port = relay.getAddress().getPort();
      Assert.assertTrue(port == 11110 || port == 11120);
      Assert.assertEquals(e.getValue().size(), 1); // one partition for each relay

      // debug print
      LOG.debug("relay="+relay+":");
      for(ClusterManagerResourceKey rk : e.getValue()) {
        Assert.assertEquals(rk.getPhysicalPartition(), "BizProfile");
        Assert.assertEquals(rk.getLogicalPartition(), "p7_1");
        Assert.assertTrue(!rk.getIsMaster(), "Should be SLAVE");
        LOG.debug("\t" + rk);
      }
      LOG.debug("\n");
    }
  }


  // not a unit test - requires real CM
  //@Test
  public void testSqlInstancePortConfig() throws ClusterManagerUtilsException, InvalidConfigException, RplDbusException {
    // simulate onExternalViewChange call
    ExternalView ev3 = TestRplDbusCMAdapter.getExternalView(3);

    // connect to real ZK, but push fake external view
    RplDbusCMAdapter cmAdapter = new RplDbusCMAdapter("relayIntTemp", "localhost:2181");
    cmAdapter.connect();
    RplDbusMySqlInstanceRegistry mySqlPorts = new RplDbusMySqlInstanceRegistry(null, cmAdapter);
    List<ExternalView> evList = new ArrayList<ExternalView>(1);
    evList.add(ev3);

    cmAdapter.onExternalViewChange(evList, null);

    //
    Map<ClusterManagerRelayCoordinates, List<ClusterManagerResourceKey>> view = cmAdapter.getCurrentExternalViews();
    Assert.assertEquals(view.size(), 2, "2 relays");
    for(Map.Entry<ClusterManagerRelayCoordinates, List<ClusterManagerResourceKey>> e : view.entrySet()) {
      for(ClusterManagerResourceKey rk : e.getValue()) {
        String phSource = rk.getPhysicalSource();
        RplDbusNodeCoordinates node = mySqlPorts.getMysqlInstance(new RplDbusNodeCoordinates(phSource));
        LOG.debug("For rk=" + rk + " got node = " + node);
      }
    }
    cmAdapter.disconnect();
  }

  public static ExternalView getExternalView(int evNum) {
    String ev = _testEVs[evNum];
    ZNRecordSerializer zs = new ZNRecordSerializer();
    ZNRecord zr = (ZNRecord) zs.deserialize(ev.getBytes());
    return new ExternalView(zr);
  }

}
