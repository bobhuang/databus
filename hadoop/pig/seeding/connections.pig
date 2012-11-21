REGISTER avro-1.4.1.jar
REGISTER linkedin-pig-0.9.1.jar
REGISTER voldemort-0.90.li8.jar
REGISTER joda-time-1.6.jar
REGISTER google-collect-1.0-rc2.jar
REGISTER piggybank-li-0.9.1.jar

--fs -rmr /user/bvaradar/connections

-- Input fields : SourceId, DestId, Active, CreateDate, ModifiedDate, Txn
avro = LOAD '$indir' USING AvroStorage('schema','{"type":"record","name":"CONNECTIONS","namespace":"CONNS","fields":[{"name":"SOURCE_ID","type":"long","doc":"","default":0,"attributes_json":"{\'type\':\'NUMBER\',\'size\':22,\'prec\':2,\'scale\':0,\'delta\':false,\'pk\':true,\'nullable\':false}"},{"name":"DEST_ID","type":"long","doc":"","default":0,"attributes_json":"{\'type\':\'NUMBER\',\'size\':22,\'prec\':2,\'scale\':0,\'delta\':false,\'pk\':true,\'nullable\':false}"},{"name":"ACTIVE","type":["null","string"],"doc":"","default":null,"attributes_json":"{\'type\':\'CHAR\',\'size\':4,\'delta\':false,\'pk\':false,\'nullable\':true}"},{"name":"CREATE_DATE","type":["null","long"],"doc":"","default":null,"attributes_json":"{\'type\':\'DATE\',\'size\':7,\'delta\':false,\'pk\':false,\'nullable\':true}"},{"name":"MODIFIED_DATE","type":["null","long"],"doc":"","default":null,"attributes_json":"{\'type\':\'TIMESTAMP\',\'size\':11,\'scale\':6,\'delta\':true,\'pk\':false,\'nullable\':true}"},{"name":"TXN","type":["null","long"],"doc":"","default":null,"attributes_json":"{\'type\':\'NUMBER\',\'size\':22,\'prec\':2,\'scale\':0,\'delta\':false,\'pk\':false,\'nullable\':true}"}],"attributes_json":"{\'instance\':\'PCONNS\',\'dumpdate\':\'20120130211302\',\'isFull\':false,\'begin_date\':\'20120130170125\',\'end_date\':\'20120130211159\',\'total_records\':-1,\'oracle_scn\':\'\',\'oracle_time\':-1}"}');

-- Output fields : TXN, KEY (SourceId_DestId), SourceId, DestId, ACTIVE, ModifiedDate, CreateDate
out2 = FOREACH avro GENERATE $5, CONCAT(CONCAT((chararray)$0,'_'),(chararray)$1), $0, $1, $2, $3, $4;

out = order out2 by $1 parallel 100;

STORE out INTO '$outdir' Using AvroStorage('schema','{  "name" : "Connections_V2",  "doc" : "Auto-generated Avro schema for conns.sy\$connections. Generated at Feb 10, 2012 05:39:40 PM PST",  "type" : "record",  "meta" : "dbFieldName=conns.sy\$connections;",  "namespace" : "com.linkedin.events.conns",  "fields" : [ {    "name" : "txn",    "type" : [ "long", "null" ],    "meta" : "dbFieldName=TXN;dbFieldPosition=0;"  }, {    "name" : "key",    "type" : [ "string", "null" ],    "meta" : "dbFieldName=KEY;dbFieldPosition=1;"  }, {    "name" : "sourceId",    "type" : [ "long", "null" ],    "meta" : "dbFieldName=SOURCE_ID;dbFieldPosition=2;"  }, {    "name" : "destId",    "type" : [ "long", "null" ],    "meta" : "dbFieldName=DEST_ID;dbFieldPosition=3;"  }, {    "name" : "active",    "type" : [ "string", "null" ],    "meta" : "dbFieldName=ACTIVE;dbFieldPosition=4;"  }, {    "name" : "modifiedDate",    "type" : [ "long", "null" ],    "meta" : "dbFieldName=MODIFIED_DATE;dbFieldPosition=5;"  }, {    "name" : "createDate",    "type" : [ "long", "null" ],    "meta" : "dbFieldName=CREATE_DATE;dbFieldPosition=6;"  } ]}');

-- Dump 20 records to STDOUT
check = LOAD '$outdir' using AvroStorage('schema','{  "name" : "Connections_V2",  "doc" : "Auto-generated Avro schema for conns.sy\$connections. Generated at Feb 10, 2012 05:39:40 PM PST",  "type" : "record",  "meta" : "dbFieldName=conns.sy\$connections;",  "namespace" : "com.linkedin.events.conns",  "fields" : [ {    "name" : "txn",    "type" : [ "long", "null" ],    "meta" : "dbFieldName=TXN;dbFieldPosition=0;"  }, {    "name" : "key",    "type" : [ "string", "null" ],    "meta" : "dbFieldName=KEY;dbFieldPosition=1;"  }, {    "name" : "sourceId",    "type" : [ "long", "null" ],    "meta" : "dbFieldName=SOURCE_ID;dbFieldPosition=2;"  }, {    "name" : "destId",    "type" : [ "long", "null" ],    "meta" : "dbFieldName=DEST_ID;dbFieldPosition=3;"  }, {    "name" : "active",    "type" : [ "string", "null" ],    "meta" : "dbFieldName=ACTIVE;dbFieldPosition=4;"  }, {    "name" : "modifiedDate",    "type" : [ "long", "null" ],    "meta" : "dbFieldName=MODIFIED_DATE;dbFieldPosition=5;"  }, {    "name" : "createDate",    "type" : [ "long", "null" ],    "meta" : "dbFieldName=CREATE_DATE;dbFieldPosition=6;"  } ]}');
val = limit check 20;
dump val;
