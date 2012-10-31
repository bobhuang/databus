/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.events.company;

@SuppressWarnings("all")
public class COMPANY_REVENUES_T extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"COMPANY_REVENUES_T\",\"namespace\":\"com.linkedin.events.company\",\"fields\":[{\"name\":\"companyRevenues\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"companyRevenueT\",\"fields\":[{\"name\":\"crudType\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=CRUD_TYPE;dbFieldPosition=0;\"},{\"name\":\"revenueId\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=REVENUE_ID;dbFieldPosition=1;\"},{\"name\":\"companyId\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=COMPANY_ID;dbFieldPosition=2;\"},{\"name\":\"revenueAmount\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=REVENUE_AMOUNT;dbFieldPosition=3;\"},{\"name\":\"currencyCode\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=CURRENCY_CODE;dbFieldPosition=4;\"},{\"name\":\"revenueYear\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=REVENUE_YEAR;dbFieldPosition=5;\"},{\"name\":\"changesetId\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=CHANGESET_ID;dbFieldPosition=6;\"}],\"meta\":\"dbFieldName=COMPANY_REVENUES;dbFieldPosition=0;\"}}}]}");
  public java.util.List<com.linkedin.events.company.companyRevenueT> companyRevenues;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return companyRevenues;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: companyRevenues = (java.util.List<com.linkedin.events.company.companyRevenueT>)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
