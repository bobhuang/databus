REGISTER avro-1.4.1.jar
REGISTER linkedin-pig-0.9.1.jar
REGISTER voldemort-0.90.li8.jar
REGISTER joda-time-1.6.jar
REGISTER google-collect-1.0-rc2.jar
REGISTER piggybank-li-0.9.1.jar

--fs -rmr /user/bvaradar/connectionscnt

-- Input fields : MemberId, Cnt, Txn
avro = LOAD '$infile' USING AvroStorage('schema','{"type":"record","name":"CONNECTIONS_CNT","namespace":"CONNS","fields":[{"name":"MEMBER_ID","type":"long","doc":"","default":0,"attributes_json":"{\'type\':\'NUMBER\',\'size\':22,\'prec\':2,\'scale\':0,\'delta\':false,\'pk\':true,\'nullable\':false}"},{"name":"CNT","type":["null","long"],"doc":"","default":null,"attributes_json":"{\'type\':\'NUMBER\',\'size\':22,\'prec\':2,\'scale\':0,\'delta\':false,\'pk\':false,\'nullable\':true}"},{"name":"TXN","type":"long","doc":"","default":0,"attributes_json":"{\'type\':\'NUMBER\',\'size\':22,\'prec\':2,\'scale\':0,\'delta\':false,\'pk\':false,\'nullable\':false}"}],"attributes_json":"{\'instance\':\'PCONNS\',\'dumpdate\':\'20120128033417\',\'isFull\':true,\'begin_date\':\'20120128032417\',\'end_date\':\'20120128033631\',\'total_records\':125702033,\'oracle_scn\':\'\',\'oracle_time\':-1}"}');

-- Output fields : TXN, KEY (MemberId_Cnt),MemberId,Cnt
out2 = FOREACH avro GENERATE $2, CONCAT(CONCAT((chararray)$0,'_'),(($1 is null) ? '' : (chararray)$1)), $0, $1;

out = order out2 by $1 parallel 100;

STORE out INTO '$outfile' Using AvroStorage('schema','{ "name" : "ConnectionsCnt_V2", "doc" : "Auto-generated Avro schema for conns.sy\$connections_cnt. Generated at Feb 10, 2012 05:39:57 PM PST", "type" : "record", "meta" : "dbFieldName=conns.sy\$connections_cnt;", "namespace" : "com.linkedin.events.conns", "fields" : [ { "name" : "txn", "type" : [ "long", "null" ], "meta" : "dbFieldName=TXN;dbFieldPosition=0;" }, { "name" : "key", "type" : [ "string", "null" ], "meta" : "dbFieldName=KEY;dbFieldPosition=1;" }, { "name" : "memberId", "type" : [ "long", "null" ], "meta" : "dbFieldName=MEMBER_ID;dbFieldPosition=2;" }, { "name" : "cnt", "type" : [ "long", "null" ], "meta" : "dbFieldName=CNT;dbFieldPosition=3;" } ]}');

-- Dump 2000 records to STDOUT
check = LOAD '$outfile' using AvroStorage('schema','{ "name" : "ConnectionsCnt_V2", "doc" : "Auto-generated Avro schema for conns.sy\$connections_cnt. Generated at Feb 10, 2012 05:39:57 PM PST", "type" : "record", "meta" : "dbFieldName=conns.sy\$connections_cnt;", "namespace" : "com.linkedin.events.conns", "fields" : [ { "name" : "txn", "type" : [ "long", "null" ], "meta" : "dbFieldName=TXN;dbFieldPosition=0;" }, { "name" : "key", "type" : [ "string", "null" ], "meta" : "dbFieldName=KEY;dbFieldPosition=1;" }, { "name" : "memberId", "type" : [ "long", "null" ], "meta" : "dbFieldName=MEMBER_ID;dbFieldPosition=2;" }, { "name" : "cnt", "type" : [ "long", "null" ], "meta" : "dbFieldName=CNT;dbFieldPosition=3;" } ]}');
val = limit check 2000;
dump val;
