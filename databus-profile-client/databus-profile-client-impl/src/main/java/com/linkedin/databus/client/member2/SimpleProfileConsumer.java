package com.linkedin.databus.client.member2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.databus.client.generic.DatabusFileLoggingConsumer;
import com.linkedin.databus.client.generic.SimpleFileLoggingConsumer;

public class SimpleProfileConsumer extends SimpleFileLoggingConsumer {

  public static final String MODULE = SimpleProfileConsumer.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public static final String MEMBER_PROFILE_SOURCE_NAME = "com.linkedin.events.member2.profile.MemberProfile";
  public static final String MEMBER_ACCOUNT_SOURCE_NAME = "com.linkedin.events.member2.account.MemberAccount";
  public static final String MEMBER_SETTING_SOURCE_NAME = "com.linkedin.events.member2.setting.MemberSetting";
  public static final String MEMBER_BUSINESS_ATTR_SOURCE_NAME = "com.linkedin.events.member2.businessattr.MemberBusinessAttr";

  protected String[] addSources()
  {
	String[] sources = new String[] {MEMBER_PROFILE_SOURCE_NAME};
    // cannot enable this if source does not have it. Or it will hit DDS-417.
    //sources.add(MEMBER_ACCOUNT_SOURCE_NAME);
    //sources.add(MEMBER_SETTING_SOURCE_NAME);
    //sources.add(MEMBER_BUSINESS_ATTR_SOURCE_NAME);
    return sources;
  }

  protected DatabusFileLoggingConsumer createTypedConsumer(String valueDumpFile) throws IOException
  {
    return new DatabusProfileConsumer(valueDumpFile);
  }
  public static void main(String args[]) throws Exception
  {
    SimpleProfileConsumer simpleProfileConsumer = new SimpleProfileConsumer();
    simpleProfileConsumer.mainFunction(args);
  }

}
