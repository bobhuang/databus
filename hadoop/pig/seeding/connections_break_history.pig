REGISTER avro-1.4.1.jar
REGISTER linkedin-pig-0.9.1.jar
REGISTER voldemort-0.90.li8.jar
REGISTER joda-time-1.6.jar
REGISTER google-collect-1.0-rc2.jar
REGISTER piggybank-li-0.9.1.jar

--fs -rmr /user/bvaradar/connectionsbreakhistory

-- Input fields : BreakerId, BreakeeId, CreateDate,IsFromCS,ModifiedDate,txn
avro = LOAD '$infile' USING AvroStorage('schema','{"type":"record","name":"CONNECTION_BREAK_HISTORY","namespace":"CONNS","fields":[{"name":"BREAKER_ID","type":["null","long"],"doc":"","default":null,"attributes_json":"{\'type\':\'NUMBER\',\'size\':22,\'prec\':2,\'scale\':0,\'delta\':false,\'pk\':false,\'nullable\':true}"},{"name":"BREAKEE_ID","type":["null","long"],"doc":"","default":null,"attributes_json":"{\'type\':\'NUMBER\',\'size\':22,\'prec\':2,\'scale\':0,\'delta\':false,\'pk\':false,\'nullable\':true}"},{"name":"CREATE_DATE","type":["null","long"],"doc":"","default":null,"attributes_json":"{\'type\':\'TIMESTAMP\',\'size\':11,\'scale\':6,\'delta\':false,\'pk\':false,\'nullable\':true}"},{"name":"IS_FROM_CS","type":["null","string"],"doc":"","default":null,"attributes_json":"{\'type\':\'CHAR\',\'size\':4,\'delta\':false,\'pk\':false,\'nullable\':true}"},{"name":"MODIFIED_DATE","type":["null","long"],"doc":"","default":null,"attributes_json":"{\'type\':\'TIMESTAMP\',\'size\':11,\'scale\':6,\'delta\':false,\'pk\':false,\'nullable\':true}"},{"name":"TXN","type":"long","doc":"","default":0,"attributes_json":"{\'type\':\'NUMBER\',\'size\':22,\'prec\':2,\'scale\':0,\'delta\':false,\'pk\':false,\'nullable\':false}"}],"attributes_json":"{\'instance\':\'PCONNS\',\'dumpdate\':\'20120128033203\',\'isFull\':true,\'begin_date\':\'20120128032203\',\'end_date\':\'20120128033308\',\'total_records\':27737552,\'oracle_scn\':\'\',\'oracle_time\':-1}"}');

-- Output fields : TXN,KEY(breakerId_breakeeId_CreateDate),breakerId,breakeeId,IsFromCS,CreateDate
out2 = FOREACH avro GENERATE $5, CONCAT(CONCAT(CONCAT(CONCAT((chararray)$0,'_'),(chararray)$1),'_'),(chararray)$2),$0,$1,$3,$2;

out = order out2 by $1 parallel 100;

STORE out INTO '$outfile' Using AvroStorage('schema','{ "name" : "ConnectionBreakHistory_V2", "doc" : "Auto-generated Avro schema for conns.sy\$connection_break_history. Generated at Feb 10, 2012 05:39:48 PM PST", "type" : "record", "meta" : "dbFieldName=conns.sy\$connection_break_history;", "namespace" : "com.linkedin.events.conns", "fields" : [ { "name" : "txn", "type" : [ "long", "null" ], "meta" : "dbFieldName=TXN;dbFieldPosition=0;" }, { "name" : "key", "type" : [ "string", "null" ], "meta" : "dbFieldName=KEY;dbFieldPosition=1;" }, { "name" : "breakerId", "type" : [ "long", "null" ], "meta" : "dbFieldName=BREAKER_ID;dbFieldPosition=2;" }, { "name" : "breakeeId", "type" : [ "long", "null" ], "meta" : "dbFieldName=BREAKEE_ID;dbFieldPosition=3;" }, { "name" : "isFromCs", "type" : [ "string", "null" ], "meta" : "dbFieldName=IS_FROM_CS;dbFieldPosition=4;" }, { "name" : "createDate", "type" : [ "long", "null" ], "meta" : "dbFieldName=CREATE_DATE;dbFieldPosition=5;" } ]}');

-- Dump 20 records to STDOUT
check = LOAD '$outfile' Using AvroStorage('schema','{ "name" : "ConnectionBreakHistory_V2", "doc" : "Auto-generated Avro schema for conns.sy\$connection_break_history. Generated at Feb 10, 2012 05:39:48 PM PST", "type" : "record", "meta" : "dbFieldName=conns.sy\$connection_break_history;", "namespace" : "com.linkedin.events.conns", "fields" : [ { "name" : "txn", "type" : [ "long", "null" ], "meta" : "dbFieldName=TXN;dbFieldPosition=0;" }, { "name" : "key", "type" : [ "string", "null" ], "meta" : "dbFieldName=KEY;dbFieldPosition=1;" }, { "name" : "breakerId", "type" : [ "long", "null" ], "meta" : "dbFieldName=BREAKER_ID;dbFieldPosition=2;" }, { "name" : "breakeeId", "type" : [ "long", "null" ], "meta" : "dbFieldName=BREAKEE_ID;dbFieldPosition=3;" }, { "name" : "isFromCs", "type" : [ "string", "null" ], "meta" : "dbFieldName=IS_FROM_CS;dbFieldPosition=4;" }, { "name" : "createDate", "type" : [ "long", "null" ], "meta" : "dbFieldName=CREATE_DATE;dbFieldPosition=5;" } ]}');
val = limit check 20;
dump val;
