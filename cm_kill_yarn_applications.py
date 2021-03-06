#!/opt/cloudera/parcels/Anaconda/bin/python

## *******************************************************************************************
##  cm_kill_yarn_applications.py
##
##  Kills Long Running YARN Applications
##
##  Usage: ./cm_kill_yarn_applications.py  query_running_seconds [KILL]
##
##    Set query_running_seconds to the threshold considered "too long"
##    for a YARN application to run, so that queries that have been running
##    longer than that will be identifed as queries to be killed
##
##    The second argument "KILL" is optional
##    Without this argument, no queries will actually be killed, instead a list
##    of queries that are identified as running too long will just be printed to the console
##    If the argument "KILL" is provided a cancel command will be issues for each selcted query
##
##    CM versions <= 5.4 require Full Administrator role to cancel YARN applications
##
##    Set the CM URL, Cluster Name, login and password in the settings below
##
##    This script assumes there is only a single YARN service per cluster
##
## *******************************************************************************************


## ** imports *******************************

import sys
import ssl
from datetime import datetime, timedelta
from cm_api.api_client import ApiResource
from pprint import pprint

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
  print "Usage: cm_kill_yarn_applications.py <query_running_seconds> [KILL]"
  print "Example that lists applications that have run more than 10 minutes:"
  print "./cm_kill_yarn_applications.py 600"
  print "Example that kills queries that have run more than 10 minutes:"
  print "./cm_kill_yarn_applications.py 600 KILL"

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
context = ssl.create_default_context(cafile='/opt/cloudera/security/x509/cacerts.pem')
api = ApiResource(server_host=cm_host, server_port=cm_port, username=cm_login, password=cm_password, use_tls=True, ssl_context=context, version=13)

## Get the Cluster
cluster = api.get_cluster(cluster_name)

## Get the YARN service
service_list = cluster.get_all_services()
for service in service_list:
  if service.type == "YARN":
    yarn_service = service
    print "Located YARN Service: " + service.name
    break

if yarn_service is None:
  print "Error: Could not locate YARN Service"
  quit(1)

## A window of one day assumes queries have not been running more than 24 hours
now = datetime.utcnow()
start = now - timedelta(days=1)

print "Looking for YARN applications running more than " + str(query_running_seconds) + " seconds"

if kill:
  print "Queries will be killed"

filterStr = 'application_duration > ' + query_running_seconds + 's'

yarn_application_response = yarn_service.get_yarn_applications(start_time=start, end_time=now, filter_str=filterStr, limit=10)
#print yarn_application_response.applications[0]
for i in range(10):
  pprint(vars(yarn_application_response.applications[i]))
  application_attr = yarn_application_response.get_yarn_application_attributes
  print application_attr

print "done"
