/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.events.mbrrec;

@SuppressWarnings("all")
/** Auto-generated Avro schema for MBRREC.SY$MEMBER_REFERENCES. Generated at Feb 10, 2012 05:40:49 PM PST */
public class MemberReferences_V2 extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"MemberReferences_V2\",\"namespace\":\"com.linkedin.events.mbrrec\",\"fields\":[{\"name\":\"txn\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=TXN;dbFieldPosition=0;\"},{\"name\":\"key\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=KEY;dbFieldPosition=1;\"},{\"name\":\"memberId\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=MEMBER_ID;dbFieldPosition=2;\"},{\"name\":\"numRecommenders\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=NUM_RECOMMENDERS;dbFieldPosition=3;\"}],\"meta\":\"dbFieldName=MBRREC.SY$MEMBER_REFERENCES;\"}");
  public java.lang.Long txn;
  public java.lang.Long key;
  public java.lang.Long memberId;
  public java.lang.Long numRecommenders;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return txn;
    case 1: return key;
    case 2: return memberId;
    case 3: return numRecommenders;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: txn = (java.lang.Long)value$; break;
    case 1: key = (java.lang.Long)value$; break;
    case 2: memberId = (java.lang.Long)value$; break;
    case 3: numRecommenders = (java.lang.Long)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
