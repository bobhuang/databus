/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.events.conns;

@SuppressWarnings("all")
/** Auto-generated Avro schema for conns.sy$connection_break_history. Generated at Sep 15, 2011 09:43:12 AM PDT */
public class ConnectionBreakHistory_V1 extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"ConnectionBreakHistory_V1\",\"namespace\":\"com.linkedin.events.conns\",\"fields\":[{\"name\":\"txn\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=TXN;dbFieldPosition=0;\"},{\"name\":\"key\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=KEY;dbFieldPosition=1;\"},{\"name\":\"breakerId\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=BREAKER_ID;dbFieldPosition=2;\"},{\"name\":\"breakeeId\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=BREAKEE_ID;dbFieldPosition=3;\"},{\"name\":\"isFromCs\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=IS_FROM_CS;dbFieldPosition=4;\"},{\"name\":\"createDate\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=CREATE_DATE;dbFieldPosition=5;\"}],\"meta\":\"dbFieldName=conns.sy$connection_break_history;\"}");
  public java.lang.Integer txn;
  public java.lang.CharSequence key;
  public java.lang.Integer breakerId;
  public java.lang.Integer breakeeId;
  public java.lang.CharSequence isFromCs;
  public java.lang.Long createDate;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return txn;
    case 1: return key;
    case 2: return breakerId;
    case 3: return breakeeId;
    case 4: return isFromCs;
    case 5: return createDate;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: txn = (java.lang.Integer)value$; break;
    case 1: key = (java.lang.CharSequence)value$; break;
    case 2: breakerId = (java.lang.Integer)value$; break;
    case 3: breakeeId = (java.lang.Integer)value$; break;
    case 4: isFromCs = (java.lang.CharSequence)value$; break;
    case 5: createDate = (java.lang.Long)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
