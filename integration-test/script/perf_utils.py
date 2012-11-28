#!/usr/bin/env python

'''
  Generate event
  Can also load from a file
  Will handle remote file load in the future

'''
__author__ = "Phanindra Ganti"
__version__ = "$Revision: 0.1 $"
__date__ = "$Date: 2011/9/16 $"

import pdb
from utility import *
import string
        
relayHost = "esv4-be38.corp.linkedin.com"
relayPort = "9000"
consumerHost = "esv4-be39.corp.linkedin.com"
consumerPort = "10566"

def computeRelayStats(f,cpf,pr):
    url_template = "http://%s:%s/relayStats/outbound/http/total"

    try:
	numStreamCalls = http_get_field(url_template, relayHost, relayPort, "numStreamCalls")
    	latencyStreamCalls = http_get_field(url_template, relayHost, relayPort, "latencyStreamCalls")
	numScnNotFoundStream = http_get_field(url_template, relayHost, relayPort, "numScnNotFoundStream")
        numErrStream = http_get_field(url_template, relayHost, relayPort, "numErrStream")

        f.write("##relayStats/outbound/http/total\n")
        f.write("##numStreamCalls\tlatencyStreamCalls\tnumScnNotFoundStream\tnumErrStream\n");
        s = str.format("%s\t%s\t%s\t%s\t%s\t%s\n" % (cpf, pr, numStreamCalls, latencyStreamCalls, numScnNotFoundStream, numErrStream))
        f.write(s)
        f.flush()
    finally:
        pass

def computeClientStats(f,cpf,pr,count):
    try:
         url_template = "http://%s:%s/clientStats/inbound/http/total"
   	 numStreamCalls = http_get_field(url_template, consumerHost, consumerPort, "numStreamCalls")
         latencyStreamCalls = http_get_field(url_template, consumerHost, consumerPort, "latencyStreamCalls")
	 numScnNotFoundStream = http_get_field(url_template, consumerHost, consumerPort, "numScnNotFoundStream")
         numErrStream = http_get_field(url_template, consumerHost, consumerPort, "numErrStream")

         url_template_2 = "http://%s:%s/containerStats/inbound/events/total"
         numDataEvents = http_get_field(url_template_2, consumerHost, consumerPort, "numDataEvents")
         latencyEvent  = http_get_field(url_template_2, consumerHost, consumerPort, "latencyEvent")

         url_template_3 = "http://%s:%s/containerStats/outbound/events/total"
         numDataEvents_3 = http_get_field(url_template_3, relayHost, relayPort, "numDataEvents")
         latencyEvent_3  = http_get_field(url_template_3, relayHost, relayPort, "latencyEvent")

         f.write("##clientStats/inbound/http/total and containerStats/inbound/events/total\n")
         f.write("##ConsumerPollFrequency\tProdRate\tRun\tnumStreamCalls\tlatencyStreamCalls\tnumScnNotFoundStream\tnumErrStream\tnumDataEvents\tlatencyEvent\tnumDataEvents_relay\tlatencyEvent_relay\n")
         s = str.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (cpf, pr, count, numStreamCalls, latencyStreamCalls, numScnNotFoundStream, numErrStream, numDataEvents, latencyEvent, numDataEvents_3, latencyEvent_3))
         f.write(s)
	 f.flush()
    finally:
         pass

# client pollfrequency, prod_rate
def main(cpf, pr):
    if not os.path.exists("integration-test/var/log/perf"):
        os.makedirs("integration-test/var/log/perf")
     
    filename = "integration-test/var/log/perf/run_%s_%s" % (cpf, pr)
    print filename
    f = open(filename, 'a')
    try:
    	for i in range(1,20):
      		computeClientStats(f,cpf,pr,i)
      		# Sleep for a minute before regathering stats
      		time.sleep(30);
    finally:
    	f.close()
    return
 
if __name__ == "__main__":
    print "arg1 is (%s)" % sys.argv[1], "arg2 is (%s)" % sys.argv[2]
    main(sys.argv[1], sys.argv[2])
