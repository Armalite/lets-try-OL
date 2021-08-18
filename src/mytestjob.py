#!/user/bin/env python3 -tt
# Imports
import sys
#import os

import requests
import datetime
import json
  

#API_ENDPOINT = "http://localhost:5000/api/v1/lineage"
API_KEY = "XXXXXXXXXXXXXXXXX"

      
def submit_event(eventtype):
    API_ENDPOINT = "http://localhost:5000/api/v1/lineage"
    current_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f+12:00")

    head = {'content-type': 'application/json'}

    if eventtype == "start":
        print("Submitting a START event")
        json_content = {
            "eventType": "START",
            "eventTime": f"{current_time}",
            "run": {
            "runId": "d46e465b-d358-4d32-83d4-df660ff614dd"
            },
            "job": {
            "namespace": "my-namespace",
            "name": "my-job"
            },
            "inputs": [{
            "namespace": "my-namespace",
            "name": "my-input"
            }],  
            "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client"
        }    
        content = json.dumps(json_content)
        print(content)
        return requests.post(url = API_ENDPOINT, data = content, headers=head)
    elif eventtype ==  "complete":
        print("Submitting a COMPLETE event")
        json_content = {
            "eventType": "COMPLETE",
            "eventTime": f"{current_time}",
            "run": {
            "runId": "d46e465b-d358-4d32-83d4-df660ff614dd"
            },
            "job": {
            "namespace": "my-namespace",
            "name": "my-job"
            },
            "outputs": [{
            "namespace": "my-namespace",
            "name": "my-output",
            "facets": {
                "schema": {
                "_producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/spec/OpenLineage.json#/definitions/SchemaDatasetFacet",
                "fields": [
                    { "name": "a", "type": "VARCHAR"},
                    { "name": "b", "type": "VARCHAR"}
                ]
                }
            }
            }],     
            "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client"
        }
        content = json.dumps(json_content)
        print(content)
        return requests.post(url = API_ENDPOINT, data = content, headers=head)
     
    
    #r = requests.post(url = API_ENDPOINT, data = content, headers=head)
    #print(r)

if __name__ == '__main__':
    r = submit_event(eventtype="start")
    print(r)
    r = submit_event(eventtype="complete") 
    print(r)

#def main():
   # args = sys.argv[1:]

    #if not args:
       # print('usage: [--flags options] [inputs] ')
       # sys.exit(1)
    
    
    # extracting response text 
    #pastebin_url = r.text
    #print("The pastebin URL is:%s"%pastebin_url)


# Main body
