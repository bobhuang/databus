/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.events.company;
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
public class databusCompanyDescT extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"databusCompanyDescT\",\"namespace\":\"com.linkedin.events.company\",\"fields\":[{\"name\":\"descriptionId\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=DESCRIPTION_ID;dbFieldPosition=0;\"},{\"name\":\"companyId\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=COMPANY_ID;dbFieldPosition=1;\"},{\"name\":\"description\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=DESCRIPTION;dbFieldPosition=2;\"},{\"name\":\"locale\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=LOCALE;dbFieldPosition=3;\"}],\"meta\":\"dbFieldName=COMPANY_DESCRIPTIONS;dbFieldPosition=0;\"}");
  public java.lang.Long descriptionId;
  public java.lang.Long companyId;
  public java.lang.CharSequence description;
  public java.lang.CharSequence locale;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return descriptionId;
    case 1: return companyId;
    case 2: return description;
    case 3: return locale;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: descriptionId = (java.lang.Long)value$; break;
    case 1: companyId = (java.lang.Long)value$; break;
    case 2: description = (java.lang.CharSequence)value$; break;
    case 3: locale = (java.lang.CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
