/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.events.espresso.ucp;
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
/** Auto-generated Avro schema for sy$UCP_SEQUENCE. Generated at May 29, 2012 05:28:11 PM PDT */
public class UcpSequence_V1 extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"UcpSequence_V1\",\"namespace\":\"com.linkedin.events.espresso.ucp\",\"fields\":[{\"name\":\"txn\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=TXN;dbFieldPosition=0;\"},{\"name\":\"sequencename\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=SEQUENCENAME;dbFieldPosition=1;\"},{\"name\":\"timestamp\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=TIMESTAMP;dbFieldPosition=2;\"},{\"name\":\"etag\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=ETAG;dbFieldPosition=3;\"},{\"name\":\"flags\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=FLAGS;dbFieldPosition=4;\"},{\"name\":\"rstate\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=RSTATE;dbFieldPosition=5;\"},{\"name\":\"expires\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=EXPIRES;dbFieldPosition=6;\"},{\"name\":\"schemaVersion\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=SCHEMA_VERSION;dbFieldPosition=7;\"},{\"name\":\"val\",\"type\":[\"bytes\",\"null\"],\"meta\":\"dbFieldName=VAL;dbFieldPosition=8;\"}],\"meta\":\"dbFieldName=sy$UCP_SEQUENCE;\"}");
  public java.lang.Long txn;
  public java.lang.CharSequence sequencename;
  public java.lang.Long timestamp;
  public java.lang.CharSequence etag;
  public java.lang.Long flags;
  public java.lang.CharSequence rstate;
  public java.lang.Long expires;
  public java.lang.Integer schemaVersion;
  public java.nio.ByteBuffer val;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return txn;
    case 1: return sequencename;
    case 2: return timestamp;
    case 3: return etag;
    case 4: return flags;
    case 5: return rstate;
    case 6: return expires;
    case 7: return schemaVersion;
    case 8: return val;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: txn = (java.lang.Long)value$; break;
    case 1: sequencename = (java.lang.CharSequence)value$; break;
    case 2: timestamp = (java.lang.Long)value$; break;
    case 3: etag = (java.lang.CharSequence)value$; break;
    case 4: flags = (java.lang.Long)value$; break;
    case 5: rstate = (java.lang.CharSequence)value$; break;
    case 6: expires = (java.lang.Long)value$; break;
    case 7: schemaVersion = (java.lang.Integer)value$; break;
    case 8: val = (java.nio.ByteBuffer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
