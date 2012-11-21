/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.events.member2.searchprofile;

@SuppressWarnings("all")
/** Auto-generated Avro schema for sy$member_search_profile_3. Generated at Feb 10, 2012 05:37:19 PM PST */
public class MemberSearchProfile_V4 extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"MemberSearchProfile_V4\",\"namespace\":\"com.linkedin.events.member2.searchprofile\",\"fields\":[{\"name\":\"txn\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=TXN;dbFieldPosition=0;\"},{\"name\":\"key\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=KEY;dbFieldPosition=1;\"},{\"name\":\"memberId\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=MEMBER_ID;dbFieldPosition=2;\"},{\"name\":\"accountCreatedDate\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=ACCOUNT_CREATED_DATE;dbFieldPosition=3;\"},{\"name\":\"accountModifiedDate\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=ACCOUNT_MODIFIED_DATE;dbFieldPosition=4;\"},{\"name\":\"isActive\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=IS_ACTIVE;dbFieldPosition=5;\"},{\"name\":\"registrationDate\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=REGISTRATION_DATE;dbFieldPosition=6;\"},{\"name\":\"registrationFirstName\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=REGISTRATION_FIRST_NAME;dbFieldPosition=7;\"},{\"name\":\"registrationLastName\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=REGISTRATION_LAST_NAME;dbFieldPosition=8;\"},{\"name\":\"registrationMaidenName\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=REGISTRATION_MAIDEN_NAME;dbFieldPosition=9;\"},{\"name\":\"lastNamePreference\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=LAST_NAME_PREFERENCE;dbFieldPosition=10;\"},{\"name\":\"profileModifiedDate\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=PROFILE_MODIFIED_DATE;dbFieldPosition=11;\"},{\"name\":\"xmlSchemaVersion\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=XML_SCHEMA_VERSION;dbFieldPosition=12;\"},{\"name\":\"updateVersion\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=UPDATE_VERSION;dbFieldPosition=13;\"},{\"name\":\"profileAuthKey\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=PROFILE_AUTH_KEY;dbFieldPosition=14;\"},{\"name\":\"xmlContentClob\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=XML_CONTENT_CLOB;dbFieldPosition=15;\"},{\"name\":\"profPositions\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"databusProfPosT\",\"fields\":[{\"name\":\"profilePositionId\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=PROFILE_POSITION_ID;dbFieldPosition=0;\"},{\"name\":\"memberId\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=MEMBER_ID;dbFieldPosition=1;\"},{\"name\":\"createdDate\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=CREATED_DATE;dbFieldPosition=2;\"},{\"name\":\"modifiedDate\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=MODIFIED_DATE;dbFieldPosition=3;\"},{\"name\":\"xmlSchemaVersion\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=XML_SCHEMA_VERSION;dbFieldPosition=4;\"},{\"name\":\"startMonthyear\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=START_MONTHYEAR;dbFieldPosition=5;\"},{\"name\":\"endMonthyear\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=END_MONTHYEAR;dbFieldPosition=6;\"},{\"name\":\"companyType\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=COMPANY_TYPE;dbFieldPosition=7;\"},{\"name\":\"memberSelectedCompanyId\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=MEMBER_SELECTED_COMPANY_ID;dbFieldPosition=8;\"},{\"name\":\"suggestedCompanyId\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=SUGGESTED_COMPANY_ID;dbFieldPosition=9;\"},{\"name\":\"industryId\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=INDUSTRY_ID;dbFieldPosition=10;\"},{\"name\":\"companySize\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=COMPANY_SIZE;dbFieldPosition=11;\"},{\"name\":\"stockTickerSymbol\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=STOCK_TICKER_SYMBOL;dbFieldPosition=12;\"},{\"name\":\"stockExchange\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=STOCK_EXCHANGE;dbFieldPosition=13;\"},{\"name\":\"xmlContent\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=XML_CONTENT;dbFieldPosition=14;\"}],\"meta\":\"dbFieldName=PROF_POSITIONS;dbFieldPosition=16;\"}}},{\"name\":\"profEducations\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"databusProfEduT\",\"fields\":[{\"name\":\"profileEducationId\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=PROFILE_EDUCATION_ID;dbFieldPosition=0;\"},{\"name\":\"memberId\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=MEMBER_ID;dbFieldPosition=1;\"},{\"name\":\"createdDate\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=CREATED_DATE;dbFieldPosition=2;\"},{\"name\":\"modifiedDate\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=MODIFIED_DATE;dbFieldPosition=3;\"},{\"name\":\"xmlSchemaVersion\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=XML_SCHEMA_VERSION;dbFieldPosition=4;\"},{\"name\":\"startMonthyear\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=START_MONTHYEAR;dbFieldPosition=5;\"},{\"name\":\"endMonthyear\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=END_MONTHYEAR;dbFieldPosition=6;\"},{\"name\":\"schoolId\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=SCHOOL_ID;dbFieldPosition=7;\"},{\"name\":\"countryCode\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=COUNTRY_CODE;dbFieldPosition=8;\"},{\"name\":\"provinceCode\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=PROVINCE_CODE;dbFieldPosition=9;\"},{\"name\":\"xmlContent\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=XML_CONTENT;dbFieldPosition=10;\"}],\"meta\":\"dbFieldName=PROF_EDUCATIONS;dbFieldPosition=17;\"}}},{\"name\":\"profElements\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"databusProfElemT\",\"fields\":[{\"name\":\"profileElementId\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=PROFILE_ELEMENT_ID;dbFieldPosition=0;\"},{\"name\":\"memberId\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=MEMBER_ID;dbFieldPosition=1;\"},{\"name\":\"elementNum\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=ELEMENT_NUM;dbFieldPosition=2;\"},{\"name\":\"elementType\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=ELEMENT_TYPE;dbFieldPosition=3;\"},{\"name\":\"createdDate\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=CREATED_DATE;dbFieldPosition=4;\"},{\"name\":\"modifiedDate\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=MODIFIED_DATE;dbFieldPosition=5;\"},{\"name\":\"xmlSchemaVersion\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=XML_SCHEMA_VERSION;dbFieldPosition=6;\"},{\"name\":\"attribute01\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=ATTRIBUTE01;dbFieldPosition=7;\"},{\"name\":\"attribute02\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=ATTRIBUTE02;dbFieldPosition=8;\"},{\"name\":\"attribute03\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=ATTRIBUTE03;dbFieldPosition=9;\"},{\"name\":\"attribute04\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=ATTRIBUTE04;dbFieldPosition=10;\"},{\"name\":\"attribute05\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=ATTRIBUTE05;dbFieldPosition=11;\"},{\"name\":\"attribute06\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=ATTRIBUTE06;dbFieldPosition=12;\"},{\"name\":\"attribute07\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=ATTRIBUTE07;dbFieldPosition=13;\"},{\"name\":\"attribute08\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=ATTRIBUTE08;dbFieldPosition=14;\"},{\"name\":\"attribute09\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=ATTRIBUTE09;dbFieldPosition=15;\"},{\"name\":\"attribute10\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=ATTRIBUTE10;dbFieldPosition=16;\"},{\"name\":\"xmlContent\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=XML_CONTENT;dbFieldPosition=17;\"}],\"meta\":\"dbFieldName=PROF_ELEMENTS;dbFieldPosition=18;\"}}},{\"name\":\"industryId\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=INDUSTRY_ID;dbFieldPosition=19;\"},{\"name\":\"subscriberUntil\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=SUBSCRIBER_UNTIL;dbFieldPosition=20;\"},{\"name\":\"allowOpenlinkSearch\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=ALLOW_OPENLINK_SEARCH;dbFieldPosition=21;\"},{\"name\":\"requireReferral\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=REQUIRE_REFERRAL;dbFieldPosition=22;\"},{\"name\":\"proposalAccepts\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=PROPOSAL_ACCEPTS;dbFieldPosition=23;\"},{\"name\":\"countryCode\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=COUNTRY_CODE;dbFieldPosition=24;\"},{\"name\":\"postalCode\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=POSTAL_CODE;dbFieldPosition=25;\"},{\"name\":\"geoPostalCode\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=GEO_POSTAL_CODE;dbFieldPosition=26;\"},{\"name\":\"geoPlaceCode\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=GEO_PLACE_CODE;dbFieldPosition=27;\"},{\"name\":\"latitudeDeg\",\"type\":[\"float\",\"null\"],\"meta\":\"dbFieldName=LATITUDE_DEG;dbFieldPosition=28;\"},{\"name\":\"longitudeDeg\",\"type\":[\"float\",\"null\"],\"meta\":\"dbFieldName=LONGITUDE_DEG;dbFieldPosition=29;\"},{\"name\":\"regionCode\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=REGION_CODE;dbFieldPosition=30;\"},{\"name\":\"defaultLocale\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=DEFAULT_LOCALE;dbFieldPosition=31;\"},{\"name\":\"inviterMemberId\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=INVITER_MEMBER_ID;dbFieldPosition=32;\"},{\"name\":\"geoPlaceMaskCode\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=GEO_PLACE_MASK_CODE;dbFieldPosition=33;\"}],\"meta\":\"dbFieldName=sy$member_search_profile_3;\"}");
  public java.lang.Long txn;
  public java.lang.Long key;
  public java.lang.Long memberId;
  public java.lang.Long accountCreatedDate;
  public java.lang.Long accountModifiedDate;
  public java.lang.CharSequence isActive;
  public java.lang.Long registrationDate;
  public java.lang.CharSequence registrationFirstName;
  public java.lang.CharSequence registrationLastName;
  public java.lang.CharSequence registrationMaidenName;
  public java.lang.CharSequence lastNamePreference;
  public java.lang.Long profileModifiedDate;
  public java.lang.Long xmlSchemaVersion;
  public java.lang.Long updateVersion;
  public java.lang.CharSequence profileAuthKey;
  public java.lang.CharSequence xmlContentClob;
  public java.util.List<com.linkedin.events.member2.searchprofile.databusProfPosT> profPositions;
  public java.util.List<com.linkedin.events.member2.searchprofile.databusProfEduT> profEducations;
  public java.util.List<com.linkedin.events.member2.searchprofile.databusProfElemT> profElements;
  public java.lang.Long industryId;
  public java.lang.Long subscriberUntil;
  public java.lang.CharSequence allowOpenlinkSearch;
  public java.lang.CharSequence requireReferral;
  public java.lang.Long proposalAccepts;
  public java.lang.CharSequence countryCode;
  public java.lang.CharSequence postalCode;
  public java.lang.CharSequence geoPostalCode;
  public java.lang.CharSequence geoPlaceCode;
  public java.lang.Float latitudeDeg;
  public java.lang.Float longitudeDeg;
  public java.lang.Integer regionCode;
  public java.lang.CharSequence defaultLocale;
  public java.lang.Long inviterMemberId;
  public java.lang.CharSequence geoPlaceMaskCode;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return txn;
    case 1: return key;
    case 2: return memberId;
    case 3: return accountCreatedDate;
    case 4: return accountModifiedDate;
    case 5: return isActive;
    case 6: return registrationDate;
    case 7: return registrationFirstName;
    case 8: return registrationLastName;
    case 9: return registrationMaidenName;
    case 10: return lastNamePreference;
    case 11: return profileModifiedDate;
    case 12: return xmlSchemaVersion;
    case 13: return updateVersion;
    case 14: return profileAuthKey;
    case 15: return xmlContentClob;
    case 16: return profPositions;
    case 17: return profEducations;
    case 18: return profElements;
    case 19: return industryId;
    case 20: return subscriberUntil;
    case 21: return allowOpenlinkSearch;
    case 22: return requireReferral;
    case 23: return proposalAccepts;
    case 24: return countryCode;
    case 25: return postalCode;
    case 26: return geoPostalCode;
    case 27: return geoPlaceCode;
    case 28: return latitudeDeg;
    case 29: return longitudeDeg;
    case 30: return regionCode;
    case 31: return defaultLocale;
    case 32: return inviterMemberId;
    case 33: return geoPlaceMaskCode;
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
    case 3: accountCreatedDate = (java.lang.Long)value$; break;
    case 4: accountModifiedDate = (java.lang.Long)value$; break;
    case 5: isActive = (java.lang.CharSequence)value$; break;
    case 6: registrationDate = (java.lang.Long)value$; break;
    case 7: registrationFirstName = (java.lang.CharSequence)value$; break;
    case 8: registrationLastName = (java.lang.CharSequence)value$; break;
    case 9: registrationMaidenName = (java.lang.CharSequence)value$; break;
    case 10: lastNamePreference = (java.lang.CharSequence)value$; break;
    case 11: profileModifiedDate = (java.lang.Long)value$; break;
    case 12: xmlSchemaVersion = (java.lang.Long)value$; break;
    case 13: updateVersion = (java.lang.Long)value$; break;
    case 14: profileAuthKey = (java.lang.CharSequence)value$; break;
    case 15: xmlContentClob = (java.lang.CharSequence)value$; break;
    case 16: profPositions = (java.util.List<com.linkedin.events.member2.searchprofile.databusProfPosT>)value$; break;
    case 17: profEducations = (java.util.List<com.linkedin.events.member2.searchprofile.databusProfEduT>)value$; break;
    case 18: profElements = (java.util.List<com.linkedin.events.member2.searchprofile.databusProfElemT>)value$; break;
    case 19: industryId = (java.lang.Long)value$; break;
    case 20: subscriberUntil = (java.lang.Long)value$; break;
    case 21: allowOpenlinkSearch = (java.lang.CharSequence)value$; break;
    case 22: requireReferral = (java.lang.CharSequence)value$; break;
    case 23: proposalAccepts = (java.lang.Long)value$; break;
    case 24: countryCode = (java.lang.CharSequence)value$; break;
    case 25: postalCode = (java.lang.CharSequence)value$; break;
    case 26: geoPostalCode = (java.lang.CharSequence)value$; break;
    case 27: geoPlaceCode = (java.lang.CharSequence)value$; break;
    case 28: latitudeDeg = (java.lang.Float)value$; break;
    case 29: longitudeDeg = (java.lang.Float)value$; break;
    case 30: regionCode = (java.lang.Integer)value$; break;
    case 31: defaultLocale = (java.lang.CharSequence)value$; break;
    case 32: inviterMemberId = (java.lang.Long)value$; break;
    case 33: geoPlaceMaskCode = (java.lang.CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
