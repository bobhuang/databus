package com.linkedin.databus2.examples.simple_member2_client;

import com.linkedin.databus.client.DatabusHttpClientImpl;

public class SimpleMemberProfileClientMain
{
  static final String MEMBER_PROFILE_SOURCE = "com.linkedin.events.member2.MemberProfile";

  public static void main(String[] args) throws Exception
  {
    DatabusHttpClientImpl.Config configBuilder = new DatabusHttpClientImpl.Config();

    //Try to connect to a relay on localhost
    configBuilder.getRuntime().getRelay("1").setHost("localhost");
    configBuilder.getRuntime().getRelay("1").setPort(9093);
    configBuilder.getRuntime().getRelay("1").setSources(MEMBER_PROFILE_SOURCE);

    //Instantiate a client using command-line parameters if any
    DatabusHttpClientImpl client = DatabusHttpClientImpl.createFromCli(args, configBuilder);

    //register callbacks
    SimpleMemberProfileConsumer profileConsumer = new SimpleMemberProfileConsumer();
    client.registerDatabusStreamListener(profileConsumer, null, MEMBER_PROFILE_SOURCE);
    client.registerDatabusBootstrapListener(profileConsumer, null, MEMBER_PROFILE_SOURCE);

    //fire off the Databus client
    client.startAndBlock();
  }

}
