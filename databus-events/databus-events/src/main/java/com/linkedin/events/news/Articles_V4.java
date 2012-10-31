/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.events.news;

@SuppressWarnings("all")
/** Auto-generated Avro schema for news.sy$articles_4. Generated at Sep 15, 2011 02:25:55 PM PDT */
public class Articles_V4 extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"Articles_V4\",\"namespace\":\"com.linkedin.events.news\",\"fields\":[{\"name\":\"txn\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=TXN;dbFieldPosition=0;\"},{\"name\":\"articleId\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=ARTICLE_ID;dbFieldPosition=1;\"},{\"name\":\"key\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=KEY;dbFieldPosition=2;\"},{\"name\":\"title\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=TITLE;dbFieldPosition=3;\"},{\"name\":\"summary\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=SUMMARY;dbFieldPosition=4;\"},{\"name\":\"language\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=LANGUAGE;dbFieldPosition=5;\"},{\"name\":\"source\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=SOURCE;dbFieldPosition=6;\"},{\"name\":\"articleSource\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=ARTICLE_SOURCE;dbFieldPosition=7;\"},{\"name\":\"url\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=URL;dbFieldPosition=8;\"},{\"name\":\"vendorRank\",\"type\":[\"float\",\"null\"],\"meta\":\"dbFieldName=VENDOR_RANK;dbFieldPosition=9;\"},{\"name\":\"vendorArticleId\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=VENDOR_ARTICLE_ID;dbFieldPosition=10;\"},{\"name\":\"publishDate\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=PUBLISH_DATE;dbFieldPosition=11;\"},{\"name\":\"fetchDate\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=FETCH_DATE;dbFieldPosition=12;\"},{\"name\":\"articleText\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=ARTICLE_TEXT;dbFieldPosition=13;\"},{\"name\":\"signature\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=SIGNATURE;dbFieldPosition=14;\"},{\"name\":\"directUrl\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=DIRECT_URL;dbFieldPosition=15;\"},{\"name\":\"boost\",\"type\":[\"float\",\"null\"],\"meta\":\"dbFieldName=BOOST;dbFieldPosition=16;\"},{\"name\":\"active\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=ACTIVE;dbFieldPosition=17;\"},{\"name\":\"articleTopics\",\"type\":{\"type\":\"record\",\"name\":\"DATABUS_ARTICLE_TOPICS_T\",\"fields\":[{\"name\":\"articleTopics\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"databusArticleTopicT\",\"fields\":[{\"name\":\"topicId\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=TOPIC_ID;dbFieldPosition=0;\"},{\"name\":\"rank\",\"type\":[\"float\",\"null\"],\"meta\":\"dbFieldName=RANK;dbFieldPosition=1;\"}],\"meta\":\"dbFieldName=ARTICLE_TOPICS;dbFieldPosition=0;\"}}}]},\"meta\":\"dbFieldName=ARTICLE_TOPICS;dbFieldPosition=18;\"},{\"name\":\"articleSourceId\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=ARTICLE_SOURCE_ID;dbFieldPosition=19;\"},{\"name\":\"isPrivate\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=IS_PRIVATE;dbFieldPosition=20;\"}],\"meta\":\"dbFieldName=news.sy$articles_4;\"}");
  public java.lang.Integer txn;
  public java.lang.Integer articleId;
  public java.lang.Integer key;
  public java.lang.CharSequence title;
  public java.lang.CharSequence summary;
  public java.lang.CharSequence language;
  public java.lang.CharSequence source;
  public java.lang.CharSequence articleSource;
  public java.lang.CharSequence url;
  public java.lang.Float vendorRank;
  public java.lang.CharSequence vendorArticleId;
  public java.lang.Long publishDate;
  public java.lang.Long fetchDate;
  public java.lang.CharSequence articleText;
  public java.lang.CharSequence signature;
  public java.lang.CharSequence directUrl;
  public java.lang.Float boost;
  public java.lang.CharSequence active;
  public com.linkedin.events.news.DATABUS_ARTICLE_TOPICS_T articleTopics;
  public java.lang.Integer articleSourceId;
  public java.lang.CharSequence isPrivate;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return txn;
    case 1: return articleId;
    case 2: return key;
    case 3: return title;
    case 4: return summary;
    case 5: return language;
    case 6: return source;
    case 7: return articleSource;
    case 8: return url;
    case 9: return vendorRank;
    case 10: return vendorArticleId;
    case 11: return publishDate;
    case 12: return fetchDate;
    case 13: return articleText;
    case 14: return signature;
    case 15: return directUrl;
    case 16: return boost;
    case 17: return active;
    case 18: return articleTopics;
    case 19: return articleSourceId;
    case 20: return isPrivate;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: txn = (java.lang.Integer)value$; break;
    case 1: articleId = (java.lang.Integer)value$; break;
    case 2: key = (java.lang.Integer)value$; break;
    case 3: title = (java.lang.CharSequence)value$; break;
    case 4: summary = (java.lang.CharSequence)value$; break;
    case 5: language = (java.lang.CharSequence)value$; break;
    case 6: source = (java.lang.CharSequence)value$; break;
    case 7: articleSource = (java.lang.CharSequence)value$; break;
    case 8: url = (java.lang.CharSequence)value$; break;
    case 9: vendorRank = (java.lang.Float)value$; break;
    case 10: vendorArticleId = (java.lang.CharSequence)value$; break;
    case 11: publishDate = (java.lang.Long)value$; break;
    case 12: fetchDate = (java.lang.Long)value$; break;
    case 13: articleText = (java.lang.CharSequence)value$; break;
    case 14: signature = (java.lang.CharSequence)value$; break;
    case 15: directUrl = (java.lang.CharSequence)value$; break;
    case 16: boost = (java.lang.Float)value$; break;
    case 17: active = (java.lang.CharSequence)value$; break;
    case 18: articleTopics = (com.linkedin.events.news.DATABUS_ARTICLE_TOPICS_T)value$; break;
    case 19: articleSourceId = (java.lang.Integer)value$; break;
    case 20: isPrivate = (java.lang.CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
