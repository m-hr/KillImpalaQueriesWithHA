#!/usr/bin/python

##
## Kill Long Running Impala Queries
##
## Usage: ./killLongRunningImpalaQueries.py queryRunningSeconds [KILL]
##
##   Set queryRunningSeconds to the threshold considered "too long" for an Impala query to run, so that queries that have been runnung longer than that will be identified as queries to be killed 
##
##
##
##
## The second argument "KILL" is optiional 
## Without this argumanet, no queries will actually be killed, instead a list of queries that are identified as running too long will just be printed to the console
##
##if the argument "KILL" is provided a cancel command will be issues for each selected query

## CM versions < = 5.4 require Full Administrator role to cancel Impala queries
## Set the CM URL< Cluster Name, Login and Password in the settings below
## This script assumes there is only a single Impala servoce per cluster 
##********

import os
import urllib2
import re
import smtplib
import sys
from datetime import datetime,timedelta
from cm_api.api_client import ApiResource
from cm_api.endpoint import hosts
from email.mime.text import MIMEText
from subprocess import Popen, PIPE


cm_host="lcdre30394.bankofamerica.com"
cm_port="7183"

cm_login="****"
coordinator_login="*****"

cm_password="*****"
coordinator_password="****"

cluster_name="cluster"


fmt='%Y-%m-%d %H:%M:%S %Z'


def cancelImpalaQuery(url,login,password): 
	impala_url=str(url)
	login_user=login
	login_password=password
	passman=urllib2.HTTPPasswordMgrWithDefaultRealm()
	passman.add_password(None,impala_url,login_user,login_password)
	authhandler=urllib2.HTTPDigestAuthHandler(passman)
	opener=urllib2.build_opener(authhandler)
	urllib2.install_opener(opener)
	page= urllib2.urlopen(impala_url)
	data=page.read()
	print data
	
	
def printUsageMessage(): 
	print "Usage: killLongRunningImpalaQueries.py <user> <queryRunningSeconds> [KILL]"
	print "Example that lists queries that have run more than 10 minutes(Convert to seconds 10*60 = 600 seconds) for user user@cloudera.com"
	print "./kill LongRunningImpalaQueries.py user@cloudera.com 600"
	print "Example that kills queries that have run more than 10 minutes(Convert to seconds 10*60 = 600 seconds) for user@cloudera.com"
	print "./killLongRunningImpalaQueries.py user@cloudera.com 600 KILL"

#validate input params
if(len(sys.argv) ==1 or len(sys.argv) == 2 or len(sys.argv) == 3 or len(sys.argv) > 5
   printUsageMessage()
   quit(1) 
   
queryRunningSeconds=sys.argv[3]
inUser=sys.arhv[1]
inUser2=sys.argv[2]

if nor qyeryRunningSeconds.isdigit():
  print "Error: the first argument must be a digit(number of seconds) "
  print UsageMessage()
  quit(1)
  
kill=False

if len(sys.argv[4] != 'KILL': 
   print "the only valid third argument is \"KILL\""
   printUsageMessage()
   quit(1) 
else:
  kill= True
  
impala_service=None

print "\n Connecting to Cloudera Manager at " + cm_host + ":" + cm_port
api= ApiResource(server_host=cm_host,server_port=cm_port, username=cm_login, password=cm_password, use_tls=True, version=12)


cluster=api.get_cluster(cluster_name)

service_list=cluster.get_all_services()
for service in service_list: 
   if service.type == "IMPALA":
     impala_service= service
     print "Located Impala Service: " + service.name
     break
if impala_service is None:  	
   print "Error: Could not locate Impala Service"
   quit(1) 
   
   
now=datetime.utcnow()
start=now - timedelta(days=1)

print "Looking for Impala queries running more than " +  str( queryRunningSeconds) + " seconds"

if kill:
   print "Queries wll be killed"
   
filterStr1= 'queryDuration > ' + queryRunnungSeconds + 's' print "filetring string1" + filterStr1
filterStr = filterStr1 + 'AND user ="' + inUser + '"' + ' OR ' + filterStr1 + ' AND user = "' + inUser2 + '"'
print "filter string" + filterStr

impala_query_response = impala_service.get_impala_queries(start_time=start,end_time=now, filter_str=filterStr, limit 1000)
queries= impala_query_response.queries

longRunningQueryCount=0

for i in range (0,len(queries)):
   query = queries[i]
   
   if query.queryState != 'FINISHED' and query.queryState != 'EXCEPTION':
        longRunningQueryCount = longRunningQueryCount + 1
        if longRunningQueryCount ==1 :
           print '--long running queries ------'
           
           print querystate: " + query.query.State
        print "queryId:" + query.queryId
        queryId=query.queryId
        print "user:" + query.user
        print "startTime:" + query.startTime.strftime(fmt)
        query_duration= now - query.startTime
        print "query running time (seconds): " + str(query_duration.seconds + query_duration.days * 86400) 
        print "SQL: " + query.statement
        coordinator=str(query,coordinator)
        Api,Hostid=corodinator.split(":")
        Hostidnew=Hostid.strip()
        Host_id=hosts.get_host(api,Hostidbew)
        print "Coordinator Hostif in Cluster Reference ----->: %s" %Host_id
        Hostnew1=str(Host_id)
        Api1,Hostid1 = Hostnew1.split('(')
        Hostifnew1=re.sub('[(){}<>]','',Hostid1)
        print "Coordinator Ip Address--->", Hostidnew1
        #cancelImpalaQuery(impala_cancel_url,coordinator_login,coordinator_password)
        
        if kill:
           print "Attempting to kill query..."
           print "queryId: " + query,queryId
           impala_cancel_url="https://%s:25000/cancel_query?query_id=%s" %(Hostidnew1,queryId)
           print "Impala URI to cancel--->", impala_cancel_url
           
           cancelImpalaQuery(impala_cancel_url,coordinator_login,coordinator_password)
        print '--------'
        
if longRunnungQueryCount ==0 :
    print "No queries found"
    
print "done"