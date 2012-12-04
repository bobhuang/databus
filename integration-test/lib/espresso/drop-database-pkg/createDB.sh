#curl -d 'jsonParameters={"command":"addResourceGroup","resourceGroupName":"pgantiDB", "partitions":"3","stateModelDefRef":"MasterSlave"}' -H "Content-Type: application/json" http://esv4-be50.corp:12924/clusters/ESPRESSO_APP_DEV_SANDBOX/resourceGroups

espresso-tools-pkg/bin/schema-tool.sh --db-name pgantiDB --src file:///home/pganti/misc/test_schemas_registry --dst zk://esv4-be50.corp:2181/schemas_registry/ESPRESSO_APP_DEV_SANDBOX --sync
