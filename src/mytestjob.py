#!/user/bin/env python3 -tt
# Imports
import sys
#import os

import requests
import datetime
import json
  

API_ENDPOINT = "http://localhost:5000/api/v1/lineage"
API_KEY = "XXXXXXXXXXXXXXXXX"

# todo: remove hardcoded timezone
current_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f+10:00")

head = {'content-type': 'application/json'}

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
        
data = {'api_option':'paste',
        'api_paste_code':content,
        'api_paste_format':'python'}

r = requests.post(url = API_ENDPOINT, data = content, headers=head)
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
#if __name__ == '__main__':
#    main()