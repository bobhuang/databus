/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.events.news;

@SuppressWarnings("all")
/** Auto-generated Avro schema for news.sy$topics_seed_1. Generated at Sep 15, 2011 02:26:18 PM PDT */
public class TopicsSeed_V1 extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"TopicsSeed_V1\",\"namespace\":\"com.linkedin.events.news\",\"fields\":[{\"name\":\"txn\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=TXN;dbFieldPosition=0;\"},{\"name\":\"key\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=KEY;dbFieldPosition=1;\"},{\"name\":\"topicsSeedId\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=TOPICS_SEED_ID;dbFieldPosition=2;\"},{\"name\":\"topicId\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=TOPIC_ID;dbFieldPosition=3;\"},{\"name\":\"seedText\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=SEED_TEXT;dbFieldPosition=4;\"},{\"name\":\"addedDate\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=ADDED_DATE;dbFieldPosition=5;\"},{\"name\":\"updatTime\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=UPDAT_TIME;dbFieldPosition=6;\"},{\"name\":\"updatCount\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=UPDAT_COUNT;dbFieldPosition=7;\"},{\"name\":\"active\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=ACTIVE;dbFieldPosition=8;\"},{\"name\":\"type\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=TYPE;dbFieldPosition=9;\"},{\"name\":\"typeVersion\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=TYPE_VERSION;dbFieldPosition=10;\"}],\"meta\":\"dbFieldName=news.sy$topics_seed_1;\"}");
  public java.lang.Integer txn;
  public java.lang.Integer key;
  public java.lang.Integer topicsSeedId;
  public java.lang.Integer topicId;
  public java.lang.CharSequence seedText;
  public java.lang.Long addedDate;
  public java.lang.Long updatTime;
  public java.lang.Integer updatCount;
  public java.lang.CharSequence active;
  public java.lang.CharSequence type;
  public java.lang.Integer typeVersion;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return txn;
    case 1: return key;
    case 2: return topicsSeedId;
    case 3: return topicId;
    case 4: return seedText;
    case 5: return addedDate;
    case 6: return updatTime;
    case 7: return updatCount;
    case 8: return active;
    case 9: return type;
    case 10: return typeVersion;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: txn = (java.lang.Integer)value$; break;
    case 1: key = (java.lang.Integer)value$; break;
    case 2: topicsSeedId = (java.lang.Integer)value$; break;
    case 3: topicId = (java.lang.Integer)value$; break;
    case 4: seedText = (java.lang.CharSequence)value$; break;
    case 5: addedDate = (java.lang.Long)value$; break;
    case 6: updatTime = (java.lang.Long)value$; break;
    case 7: updatCount = (java.lang.Integer)value$; break;
    case 8: active = (java.lang.CharSequence)value$; break;
    case 9: type = (java.lang.CharSequence)value$; break;
    case 10: typeVersion = (java.lang.Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
