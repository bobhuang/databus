/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.events.liar.jobrelay;
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
/** Auto-generated Avro schema for SY$LIAR_JOB_RELAY_2. Generated at Sep 09, 2011 02:35:28 PM PDT */
public class LiarJobRelayV2 extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"LiarJobRelay\",\"namespace\":\"com.linkedin.events.liar.jobrelay.LiarJobRelay\",\"fields\":[{\"name\":\"txn\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=TXN;dbFieldPosition=0;\"},{\"name\":\"key\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=KEY;dbFieldPosition=1;\"},{\"name\":\"eventId\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=EVENT_ID;dbFieldPosition=2;\"},{\"name\":\"isDelete\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=IS_DELETE;dbFieldPosition=3;\"},{\"name\":\"state\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=STATE;dbFieldPosition=4;\"},{\"name\":\"isRealtime\",\"type\":[\"string\",\"null\"],\"default\":\"\",\"meta\":\"dbFieldName=IS_REALTIME;dbFieldPosition=5;\"}],\"meta\":\"dbFieldName=SY$LIAR_JOB_RELAY_2;\"}");
  public java.lang.Integer txn;
  public java.lang.Integer key;
  public java.lang.Integer eventId;
  public java.lang.CharSequence isDelete;
  public java.lang.CharSequence state;
  public java.lang.CharSequence isRealtime;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return txn;
    case 1: return key;
    case 2: return eventId;
    case 3: return isDelete;
    case 4: return state;
    case 5: return isRealtime;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: txn = (java.lang.Integer)value$; break;
    case 1: key = (java.lang.Integer)value$; break;
    case 2: eventId = (java.lang.Integer)value$; break;
    case 3: isDelete = (java.lang.CharSequence)value$; break;
    case 4: state = (java.lang.CharSequence)value$; break;
    case 5: isRealtime = (java.lang.CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
