/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.events.news;
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
/** Auto-generated Avro schema for news.SY$TAGGED_SLICES. Generated at Apr 10, 2012 11:20:36 AM PDT */
public class TaggedSlices_V1 extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"TaggedSlices_V1\",\"namespace\":\"com.linkedin.events.news\",\"fields\":[{\"name\":\"txn\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=TXN;dbFieldPosition=0;\"},{\"name\":\"key\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=KEY;dbFieldPosition=1;\"},{\"name\":\"entityId\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=ENTITY_ID;dbFieldPosition=2;\"},{\"name\":\"entityType\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=ENTITY_TYPE;dbFieldPosition=3;\"},{\"name\":\"locale\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=LOCALE;dbFieldPosition=4;\"},{\"name\":\"titleKey\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=TITLE_KEY;dbFieldPosition=5;\"},{\"name\":\"descriptionKey\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=DESCRIPTION_KEY;dbFieldPosition=6;\"},{\"name\":\"active\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=ACTIVE;dbFieldPosition=7;\"},{\"name\":\"title\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=TITLE;dbFieldPosition=8;\"},{\"name\":\"enableRss\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=ENABLE_RSS;dbFieldPosition=9;\"},{\"name\":\"logo\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=LOGO;dbFieldPosition=10;\"},{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"sliceTag\",\"fields\":[{\"name\":\"type\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=TYPE;dbFieldPosition=0;\"},{\"name\":\"value\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=VALUE;dbFieldPosition=1;\"}],\"meta\":\"dbFieldName=TAGS;dbFieldPosition=11;\"}}}],\"meta\":\"dbFieldName=news.SY$TAGGED_SLICES;pk=key;\"}");
  public java.lang.Long txn;
  public java.lang.CharSequence key;
  public java.lang.CharSequence entityId;
  public java.lang.CharSequence entityType;
  public java.lang.CharSequence locale;
  public java.lang.CharSequence titleKey;
  public java.lang.CharSequence descriptionKey;
  public java.lang.CharSequence active;
  public java.lang.CharSequence title;
  public java.lang.CharSequence enableRss;
  public java.lang.CharSequence logo;
  public java.util.List<com.linkedin.events.news.sliceTag> tags;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return txn;
    case 1: return key;
    case 2: return entityId;
    case 3: return entityType;
    case 4: return locale;
    case 5: return titleKey;
    case 6: return descriptionKey;
    case 7: return active;
    case 8: return title;
    case 9: return enableRss;
    case 10: return logo;
    case 11: return tags;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: txn = (java.lang.Long)value$; break;
    case 1: key = (java.lang.CharSequence)value$; break;
    case 2: entityId = (java.lang.CharSequence)value$; break;
    case 3: entityType = (java.lang.CharSequence)value$; break;
    case 4: locale = (java.lang.CharSequence)value$; break;
    case 5: titleKey = (java.lang.CharSequence)value$; break;
    case 6: descriptionKey = (java.lang.CharSequence)value$; break;
    case 7: active = (java.lang.CharSequence)value$; break;
    case 8: title = (java.lang.CharSequence)value$; break;
    case 9: enableRss = (java.lang.CharSequence)value$; break;
    case 10: logo = (java.lang.CharSequence)value$; break;
    case 11: tags = (java.util.List<com.linkedin.events.news.sliceTag>)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
