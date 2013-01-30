package com.linkedin.databus.client.member2;
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
