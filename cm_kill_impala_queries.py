#!/opt/cloudera/parcels/Anaconda/bin/python

## *******************************************************************************************
##  killLongRunningImpalaQueries.py
##
##  Kills Long Running Impala Queries
##
##  Usage: ./cm_kill_impala_queries.py  query_running_seconds [KILL]
##
##    Set query_running_seconds to the threshold considered "too long"
##    for an Impala query to run, so that queries that have been running
##    longer than that will be identifed as queries to be killed
##
##    The second argument "KILL" is optional
##    Without this argument, no queries will actually be killed, instead a list
##    of queries that are identified as running too long will just be printed to the console
##    If the argument "KILL" is provided a cancel command will be issues for each selcted query
##
##    CM versions <= 5.4 require Full Administrator role to cancel Impala queries
##
##    Set the CM URL, Cluster Name, login and password in the settings below
##
##    This script assumes there is only a single Impala service per cluster
##
## *******************************************************************************************


## ** imports *******************************

import sys
import ssl
from datetime import datetime, timedelta
from cm_api.api_client import ApiResource

## ** Settings ******************************

## Cloudera Manager Host
cm_host = "CHANGE_ME"
cm_port = "CHANGE_ME"

## Cloudera Manager login with Full Administrator role
cm_login = "CHANGE_ME"

## Cloudera Manager password
cm_password = 'CHANGE_ME'

## Cluster Name
cluster_name = "CHANGE_ME"

## *****************************************

fmt = '%Y-%m-%d %H:%M:%S %Z'

def print_usage_message():
  print "Usage: cm_kill_impala_queries.py <query_running_seconds> [KILL]"
  print "Example that lists queries that have run more than 10 minutes:"
  print "./cm_kill_impala_queries.py 600"
  print "Example that kills queries that have run more than 10 minutes:"
  print "./cm_kill_impala_queries.py 600 KILL"

## ** Validate command line args *************

if len(sys.argv) == 1 or len(sys.argv) > 3:
  print_usage_message()
  quit(1)

query_running_seconds = sys.argv[1]

if not query_running_seconds.isdigit():
  print "Error: the first argument must be a digit (number of seconds)"
  print_usage_message()
  quit(1)

kill = False

if len(sys.argv) == 3:
  if sys.argv[2] != 'KILL':
    print "the only valid second argument is \"KILL\""
    print_usage_message()
    quit(1)
  else:
    kill = True

impala_service = None

## Connect to CM
print "\nConnecting to Cloudera Manager at " + cm_host + ":" + cm_port
#api = ApiResource(server_host=cm_host, server_port=cm_port, username=cm_login, password=cm_password)
context = ssl.create_default_context(cafile='/opt/cloudera/security/x509/cacerts.pem')
api = ApiResource(server_host=cm_host, server_port=cm_port, username=cm_login, password=cm_password, use_tls=True, ssl_context=context, version=13)

#for c in api.get_all_clusters():
#  print c.name

## Get the Cluster
cluster = api.get_cluster(cluster_name)

## Get the IMPALA service
service_list = cluster.get_all_services()
for service in service_list:
  if service.type == "IMPALA":
    impala_service = service
    print "Located Impala Service: " + service.name
    break

if impala_service is None:
  print "Error: Could not locate Impala Service"
  quit(1)

## A window of one day assumes queries have not been running more than 24 hours
now = datetime.utcnow()
start = now - timedelta(days=1)

print "Looking for Impala queries running more than " + str(query_running_seconds) + " seconds"

if kill:
  print "Queries will be killed"

filterStr = 'query_duration > ' + query_running_seconds + 's'

impala_query_response = impala_service.get_impala_queries(start_time=start, end_time=now, filter_str=filterStr, limit=1000)
queries = impala_query_response.queries

long_running_query_count = 0

for i in range (0, len(queries)):
  query = queries[i]

  if query.queryState != 'FINISHED' and query.queryState != 'EXCEPTION':

    long_running_query_count = long_running_query_count + 1

    if long_running_query_count == 1:
      print '-- long running queries -------------'

    print "queryState : " + query.queryState
    print "queryId: " + query.queryId
    print "user: " + query.user
    print "startTime: " + query.startTime.strftime(fmt)
    query_duration = now - query.startTime
    print "query running time (seconds): " + str(query_duration.seconds + query_duration.days * 86400)
    print "SQL: " + query.statement

    if kill:
      print "Attempting to kill query..."
      impala_service.cancel_impala_query(query.queryId)

    print '-------------------------------------'

if long_running_query_count == 0:
  print "No queries found"

print "done"
