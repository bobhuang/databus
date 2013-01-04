#!/bin/bash
schema_src=$1
schema_dst=$2


prefix=$(basename $0)
mapdir=$(mktemp -dt ${prefix})
trap 'rm -r ${mapdir}' EXIT

put() {
  [ "$#" != 3 ] && exit 1
  mapname=$1; key=$2; value=$3
  [ -d "${mapdir}/${mapname}" ] || mkdir "${mapdir}/${mapname}"
  echo $value >"${mapdir}/${mapname}/${key}"
}

get() {
  [ "$#" != 2 ] && exit 1
  mapname=$1; key=$2
  cat "${mapdir}/${mapname}/${key}"
}

listKeys() {
  [ "$#" != 1 ] && exit 1
  mapname=$1;
  find ${mapdir}/${mapname} -type f | xargs -n 1 basename
}



#input: $1 == collection uri
get_resource_location()
{
  if [[ "$1" == http* ]]
  then
    # http call
    echo `curl $1$2 -v 2>&1 | strings | grep Content-Location | sed 's/^.*Content-Location: \(.*\)$/\1/' | tr -d '\015'`
  else
    # file directory
    
    find $1$2 -type f | grep json | sed "s:^.*\($2.*\):\1:"| sed 's/.json//'
  fi
}

#input: $1 == resource_uri
get_resource_content()
{
  if [[ "$1" == http* ]]
  then
    #http resource
    curl -s $1
  else
    cat $1.json
  fi
}

put_resource() 
{
  resource_uri=$1
  resource_file=$2
  echo "Resource uri = $resource_uri"
  if [[ "$resource_uri" == http* ]]
    then
      #http put
      curl -X PUT -v --data-binary @/tmp/upload_schemas/schema_content $resource_uri
    else
      #file put
      mkdir -p `dirname $resource_uri`
      cat $resource_file > $resource_uri.json
   fi
}

sync_collection()
{
  source_uri=$1
  collection_uri=$2
  dst_uri=$3
  dst_collection_uri=$4
  for resource in `get_resource_location $source_uri $collection_uri`
  do
    echo "Resource = $resource"
    swizzledResource=`echo $resource | sed "s:$collection_uri:$dst_collection_uri:"`
    echo "Swizzled Resource = $swizzledResource"
    resource_content=`get_resource_content $source_uri$resource`
    #echo "resource content = $resource_content"
    mkdir -p /tmp/upload_schemas
    echo "$resource_content" > /tmp/upload_schemas/schema_content
    put_resource $dst_uri$swizzledResource /tmp/upload_schemas/schema_content
  done
}


usage()
{
  echo "usage: `basename $0` src_schema_uri dst_schema_uri db_list"
  echo "Sample schema_uri: http://esv4-be51.corp.linkedin.com:12921 or ~/Documents/workspace/espresso-trunk/schemas_registry"
  echo "Sample db_list: MailboxDB or MailboxDB:user_MailboxDB or MailboxDB:user_MailboxDB ProfileDB:user_ProfileDB" 
  exit 1
}  

if (( "$#" < 3 ))
then
   usage $@
fi

shift 2

while (( "$#" )); do
  src=`echo $1 | cut -d ":" -f 1`
  dst=`echo $1 | cut -d ":" -f 2`
  put "swizzle" "$src" "$dst"
  shift
done
  

for dbname in `listKeys "swizzle"`
do
  echo $dbname
  swizzledName=`get "swizzle" $dbname`
  # get all the dbschemas under /schemata/db/$dbname
  sync_collection $schema_src /schemata/db/$dbname $schema_dst /schemata/db/$swizzledName
  # get all the table schemas under /schemata/table/$dbname
  sync_collection $schema_src /schemata/table/$dbname $schema_dst /schemata/table/$swizzledName
  # get all the document schemas under /schemata/document/$dbname
  sync_collection $schema_src /schemata/document/$dbname $schema_dst /schemata/document/$swizzledName
done

# some tables might refer to documents that live outside the database
#for recordType in `curl $schema_src/schemata/table/$dbname -s | grep recordType | sed 's/^.*recordType.*:.*"\(.*\)".*$/\1/'`
#do
#  sync_collection $schema_src $recordType $schema_dst ".json"
#done



