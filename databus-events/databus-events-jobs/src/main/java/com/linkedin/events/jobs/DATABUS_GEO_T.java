/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.events.jobs;
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
public class DATABUS_GEO_T extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"DATABUS_GEO_T\",\"namespace\":\"com.linkedin.events.jobs\",\"fields\":[{\"name\":\"country\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=COUNTRY;dbFieldPosition=0;\"},{\"name\":\"postalCode\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=POSTAL_CODE;dbFieldPosition=1;\"},{\"name\":\"geoPostalCode\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=GEO_POSTAL_CODE;dbFieldPosition=2;\"},{\"name\":\"regionCode\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=REGION_CODE;dbFieldPosition=3;\"},{\"name\":\"geoPlaceCode\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=GEO_PLACE_CODE;dbFieldPosition=4;\"},{\"name\":\"geoPlaceMaskCode\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=GEO_PLACE_MASK_CODE;dbFieldPosition=5;\"},{\"name\":\"latitudeDeg\",\"type\":[\"float\",\"null\"],\"meta\":\"dbFieldName=LATITUDE_DEG;dbFieldPosition=6;\"},{\"name\":\"longitudeDeg\",\"type\":[\"float\",\"null\"],\"meta\":\"dbFieldName=LONGITUDE_DEG;dbFieldPosition=7;\"}]}");
  public java.lang.CharSequence country;
  public java.lang.CharSequence postalCode;
  public java.lang.CharSequence geoPostalCode;
  public java.lang.Integer regionCode;
  public java.lang.CharSequence geoPlaceCode;
  public java.lang.CharSequence geoPlaceMaskCode;
  public java.lang.Float latitudeDeg;
  public java.lang.Float longitudeDeg;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return country;
    case 1: return postalCode;
    case 2: return geoPostalCode;
    case 3: return regionCode;
    case 4: return geoPlaceCode;
    case 5: return geoPlaceMaskCode;
    case 6: return latitudeDeg;
    case 7: return longitudeDeg;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: country = (java.lang.CharSequence)value$; break;
    case 1: postalCode = (java.lang.CharSequence)value$; break;
    case 2: geoPostalCode = (java.lang.CharSequence)value$; break;
    case 3: regionCode = (java.lang.Integer)value$; break;
    case 4: geoPlaceCode = (java.lang.CharSequence)value$; break;
    case 5: geoPlaceMaskCode = (java.lang.CharSequence)value$; break;
    case 6: latitudeDeg = (java.lang.Float)value$; break;
    case 7: longitudeDeg = (java.lang.Float)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
