#!/user/bin/env python3 -tt
# Imports
import sys
#import os

import requests
import datetime
import json 

#API_ENDPOINT = "http://localhost:5000/api/v1/lineage"
API_KEY = "XXXXXXXXXXXXXXXXX"

class ol_event:
    def __init__(self, eventtype, producer, entityname, jobname, namespace):
        self.ev_type = eventtype.upper()
        self.head = {'content-type': 'application/json'}
        self.namespace = namespace if namespace else "" #"api"
        self.job_name = jobname if jobname else "" #"api"
        self.input_name = entityname if eventtype.upper() == "START" else "" #"raw-api-appevents"
        self.output_name = entityname if eventtype.upper() == "COMPLETE" else "" #"appevent-aggregates"
        self.run_id = "d46e465b-d358-4d32-83d4-df660ff614dd"
        self.ev_producer = producer if producer else "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client"
    
    def publish():
        print("Sending to OL")


def submit_event(ol_event, api_endpoint):
    API_ENDPOINT = api_endpoint
    current_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f+12:00")

    head = {'content-type': 'application/json'}
    namespace = ol_event.namespace
    job_name = ol_event.job_name 
    input_name = ol_event.input_name 
    output_name = ol_event.output_name 
    ev_type = ol_event.ev_type
    run_id = "d46e465b-d358-4d32-83d4-df660ff614dd"
    producer = ol_event.ev_producer 

    #print(f"namespace: {namespace} job_name: {job_name}")
    if ev_type == "START":
        print("Submitting a START event")
        json_content = {
            "eventType": f"{ev_type}",
            "eventTime": f"{current_time}",
            "run": {
            "runId": "d46e465b-d358-4d32-83d4-df660ff614dd"
            },
            "job": {
            "namespace": f"{namespace}",
            "name": f"{job_name}"
            },
            "inputs": [{
            "namespace": f"{namespace}",
            "name": f"{input_name}"
            }],  
            "producer": f"{producer}"
        }    
        content = json.dumps(json_content)
        print(content)
        return requests.post(url = API_ENDPOINT, data = content, headers=head)
    elif ev_type ==  "COMPLETE":
        print("Submitting a COMPLETE event")
        json_content = {
            "eventType": "COMPLETE",
            "eventTime": f"{current_time}",
            "run": {
            "runId": "d46e465b-d358-4d32-83d4-df660ff614dd"
            },
            "job": {
            "namespace": f"{namespace}",
            "name": f"{job_name}"
            },
            "outputs": [{
            "namespace": f"{namespace}",
            "name": f"{output_name}",
            "facets": {
                "schema": {
                "_producer": f"{producer}",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/spec/OpenLineage.json#/definitions/SchemaDatasetFacet",
                "fields": [
                    { "name": "appid", "type": "VARCHAR"},
                    { "name": "appname", "type": "VARCHAR"}
                ]
                }
            }
            }, ],     
            "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client"
        }
        content = json.dumps(json_content)
        print(f"Submitted {ev_type} event. Namespace: {namespace} Job: {job_name}")
        #print(content)
        return requests.post(url = API_ENDPOINT, data = content, headers=head)
     
    
    #r = requests.post(url = API_ENDPOINT, data = content, headers=head)
    #print(r)

if __name__ == '__main__':
    prdc = "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client"
    
    startevent = ol_event(eventtype='START', producer=prdc, entityname="raw-api-appevents", jobname="enrich-api", namespace="api")
    completeevent1 = ol_event(eventtype='COMPLETE', producer=prdc, entityname="aggregated-api-appevents", jobname="enrich-api", namespace="api")
    completeevent2 = ol_event(eventtype='COMPLETE', producer=prdc, entityname="error-events", jobname="enrich-api", namespace="api")
    completeevent3 = ol_event(eventtype='COMPLETE', producer=prdc, entityname="appevent-log-output", jobname="enrich-api", namespace="api")

    r = submit_event(startevent, "http://localhost:5000/api/v1/lineage")
    r = submit_event(completeevent1, "http://localhost:5000/api/v1/lineage")
    r = submit_event(completeevent2, "http://localhost:5000/api/v1/lineage")
    r = submit_event(completeevent3, "http://localhost:5000/api/v1/lineage")
    
    #r = submit_event(eventtype="complete", producer="https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client") 
    #print(r)


#def main():
   # args = sys.argv[1:]

    #if not args:
       # print('usage: [--flags options] [inputs] ')
       # sys.exit(1)
 
