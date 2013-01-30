/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.events.anet;
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


@SuppressWarnings("all")
/** Auto-generated Avro schema for anet.sy$anet_members_5. Generated at Sep 15, 2011 03:20:18 AM PDT */
public class AnetMembers_V5 extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"AnetMembers_V5\",\"namespace\":\"com.linkedin.events.anet\",\"fields\":[{\"name\":\"txn\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=TXN;dbFieldPosition=0;\"},{\"name\":\"membershipId\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=MEMBERSHIP_ID;dbFieldPosition=1;\"},{\"name\":\"memberId\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=MEMBER_ID;dbFieldPosition=2;\"},{\"name\":\"anetId\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=ANET_ID;dbFieldPosition=3;\"},{\"name\":\"entityId\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=ENTITY_ID;dbFieldPosition=4;\"},{\"name\":\"anetType\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=ANET_TYPE;dbFieldPosition=5;\"},{\"name\":\"isPrimaryAnet\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=IS_PRIMARY_ANET;dbFieldPosition=6;\"},{\"name\":\"state\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=STATE;dbFieldPosition=7;\"},{\"name\":\"contactEmail\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=CONTACT_EMAIL;dbFieldPosition=8;\"},{\"name\":\"joinedOn\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=JOINED_ON;dbFieldPosition=9;\"},{\"name\":\"resignedOn\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=RESIGNED_ON;dbFieldPosition=10;\"},{\"name\":\"lastTransitionOn\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=LAST_TRANSITION_ON;dbFieldPosition=11;\"},{\"name\":\"createdAt\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=CREATED_AT;dbFieldPosition=12;\"},{\"name\":\"updatedAt\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=UPDATED_AT;dbFieldPosition=13;\"},{\"name\":\"locale\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=LOCALE;dbFieldPosition=14;\"},{\"name\":\"mgmtLevel\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=MGMT_LEVEL;dbFieldPosition=15;\"},{\"name\":\"mgmtTransitionOn\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=MGMT_TRANSITION_ON;dbFieldPosition=16;\"},{\"name\":\"settings\",\"type\":{\"type\":\"record\",\"name\":\"SETTINGS_T\",\"fields\":[{\"name\":\"settings\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"settingT\",\"fields\":[{\"name\":\"entityId\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=ENTITY_ID;dbFieldPosition=0;\"},{\"name\":\"settingId\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=SETTING_ID;dbFieldPosition=1;\"},{\"name\":\"settingValue\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=SETTING_VALUE;dbFieldPosition=2;\"},{\"name\":\"dateCreated\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=DATE_CREATED;dbFieldPosition=3;\"},{\"name\":\"dateModified\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=DATE_MODIFIED;dbFieldPosition=4;\"}],\"meta\":\"dbFieldName=SETTINGS;dbFieldPosition=0;\"}}}]},\"meta\":\"dbFieldName=SETTINGS;dbFieldPosition=17;\"}],\"meta\":\"dbFieldName=anet.sy$anet_members_5;pk=memberId\"}");
  public java.lang.Integer txn;
  public java.lang.Integer membershipId;
  public java.lang.Integer memberId;
  public java.lang.Integer anetId;
  public java.lang.Integer entityId;
  public java.lang.CharSequence anetType;
  public java.lang.CharSequence isPrimaryAnet;
  public java.lang.CharSequence state;
  public java.lang.CharSequence contactEmail;
  public java.lang.Long joinedOn;
  public java.lang.Long resignedOn;
  public java.lang.Long lastTransitionOn;
  public java.lang.Long createdAt;
  public java.lang.Long updatedAt;
  public java.lang.CharSequence locale;
  public java.lang.CharSequence mgmtLevel;
  public java.lang.Long mgmtTransitionOn;
  public com.linkedin.events.anet.SETTINGS_T settings;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return txn;
    case 1: return membershipId;
    case 2: return memberId;
    case 3: return anetId;
    case 4: return entityId;
    case 5: return anetType;
    case 6: return isPrimaryAnet;
    case 7: return state;
    case 8: return contactEmail;
    case 9: return joinedOn;
    case 10: return resignedOn;
    case 11: return lastTransitionOn;
    case 12: return createdAt;
    case 13: return updatedAt;
    case 14: return locale;
    case 15: return mgmtLevel;
    case 16: return mgmtTransitionOn;
    case 17: return settings;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: txn = (java.lang.Integer)value$; break;
    case 1: membershipId = (java.lang.Integer)value$; break;
    case 2: memberId = (java.lang.Integer)value$; break;
    case 3: anetId = (java.lang.Integer)value$; break;
    case 4: entityId = (java.lang.Integer)value$; break;
    case 5: anetType = (java.lang.CharSequence)value$; break;
    case 6: isPrimaryAnet = (java.lang.CharSequence)value$; break;
    case 7: state = (java.lang.CharSequence)value$; break;
    case 8: contactEmail = (java.lang.CharSequence)value$; break;
    case 9: joinedOn = (java.lang.Long)value$; break;
    case 10: resignedOn = (java.lang.Long)value$; break;
    case 11: lastTransitionOn = (java.lang.Long)value$; break;
    case 12: createdAt = (java.lang.Long)value$; break;
    case 13: updatedAt = (java.lang.Long)value$; break;
    case 14: locale = (java.lang.CharSequence)value$; break;
    case 15: mgmtLevel = (java.lang.CharSequence)value$; break;
    case 16: mgmtTransitionOn = (java.lang.Long)value$; break;
    case 17: settings = (com.linkedin.events.anet.SETTINGS_T)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
