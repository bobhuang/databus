#!/bin/bash

source=$1
relay_port=$2
bstprod_port=$3
if [ -z "$source" -o -z "${relay_port}" -o -z "${bstprod_port}" ] ; then
  echo "USAGE: $0 source relay-jetty-port bst-producer-jetty-port"
  exit 1
fi

modtopo checkout --force

echo "Creating application databus2-relay-${source} ..."
modtopo add-app --is-cfg2 --product databus2 --type jetty databus2-relay-${source}

echo "Creating application databus2-bootstrap-producer-${source} ..."
modtopo add-app --is-cfg2 --product databus2 --type jetty databus2-bootstrap-producer-${source}

echo "Adding war databus2-relay-${source}-war ..."
modtopo add-war --application databus2-relay-${source} databus2-relay-${source}-war

echo "Adding war databus2-bootstrap-producer-${source}-war ..."
modtopo add-war --application databus2-bootstrap-producer-${source} databus2-bootstrap-producer-${source}-war

echo "Adding instance i001 to databus2-relay-${source} ..."
modtopo add-instance --instance i001 --port ${relay_port} --context-path databus2-relay-${source} --tags ENG:DDS,backend,databus2,databus2-relay databus2-relay-${source}

echo "Adding instance i001 to databus2-bootstrap-producer-${source} ..."
modtopo add-instance --instance i001 --port ${bstprod_port} --context-path databus2-bootstrap-producer-${source} --tag ENG:DDS,backend,databus2,databus2-bootstrap databus2-bootstrap-producer-${source}

echo "Committing changes"
read -n 1 -p "Commit changes (Y/N)" confirm_commit
if [ "${confirm_commit}" == "y" -o "${confirm_commit}" == "Y" ] ; then
  cd ~/.deployment/modtopo
  modtopo commit -m "Adding databus2-relay-${source} and databus2-bootstrap-producer-${source} to topology"
fi

