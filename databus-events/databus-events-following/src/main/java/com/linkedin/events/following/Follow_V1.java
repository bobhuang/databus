/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.events.following;

@SuppressWarnings("all")
/** Auto-generated Avro schema for following.SY$FOLLOW. Generated at Nov 21, 2011 07:16:42 PM PST */
public class Follow_V1 extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"Follow_V1\",\"namespace\":\"com.linkedin.events.following\",\"fields\":[{\"name\":\"txn\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=TXN;dbFieldPosition=0;\"},{\"name\":\"id\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=ID;dbFieldPosition=1;\"},{\"name\":\"fromType\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=FROM_TYPE;dbFieldPosition=2;\"},{\"name\":\"fromId\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=FROM_ID;dbFieldPosition=3;\"},{\"name\":\"toType\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=TO_TYPE;dbFieldPosition=4;\"},{\"name\":\"toId\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=TO_ID;dbFieldPosition=5;\"},{\"name\":\"autofollow\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=AUTOFOLLOW;dbFieldPosition=6;\"},{\"name\":\"status\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=STATUS;dbFieldPosition=7;\"},{\"name\":\"createdOn\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=CREATED_ON;dbFieldPosition=8;\"},{\"name\":\"lastModified\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=LAST_MODIFIED;dbFieldPosition=9;\"}],\"meta\":\"dbFieldName=following.SY$FOLLOW;\"}");
  public java.lang.Integer txn;
  public java.lang.Integer id;
  public java.lang.Integer fromType;
  public java.lang.Integer fromId;
  public java.lang.Integer toType;
  public java.lang.Integer toId;
  public java.lang.Integer autofollow;
  public java.lang.CharSequence status;
  public java.lang.Long createdOn;
  public java.lang.Long lastModified;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return txn;
    case 1: return id;
    case 2: return fromType;
    case 3: return fromId;
    case 4: return toType;
    case 5: return toId;
    case 6: return autofollow;
    case 7: return status;
    case 8: return createdOn;
    case 9: return lastModified;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: txn = (java.lang.Integer)value$; break;
    case 1: id = (java.lang.Integer)value$; break;
    case 2: fromType = (java.lang.Integer)value$; break;
    case 3: fromId = (java.lang.Integer)value$; break;
    case 4: toType = (java.lang.Integer)value$; break;
    case 5: toId = (java.lang.Integer)value$; break;
    case 6: autofollow = (java.lang.Integer)value$; break;
    case 7: status = (java.lang.CharSequence)value$; break;
    case 8: createdOn = (java.lang.Long)value$; break;
    case 9: lastModified = (java.lang.Long)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}