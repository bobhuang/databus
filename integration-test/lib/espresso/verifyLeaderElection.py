import json
import urllib2

def main():
    #assumes that the REST client is started on port 2100
    cmUrl  = urllib2.urlopen('http://localhost:2100/clusters/RelayDevCluster_BizProfile/resourceGroups/relayLeaderStandby/')
    cmOp   = cmUrl.read()
    cmJson = json.loads(cmOp)
    var    = cmJson["mapFields"]["relayLeaderStandby_0"]["eat1-app25.corp.linkedin.com_11140"]
    if (var == "LEADER"):
        print "SUCCESS"
    else:
        print "FAIL"

if __name__ != "main":
    main() 
