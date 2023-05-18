# Allows us to connect to the data source and pulls the information
from sodapy import Socrata
import requests
from requests.auth import HTTPBasicAuth
import json 
import argparse
import sys
import os
import math

# Creates a parser. Parser is the thing where you add your arguments. 
parser = argparse.ArgumentParser(description='Fire Incident Dispatch Data')
# In the parse, we have two arguments to add.
# The first one is a required argument for the program to run. If page_size is not passed in, don’t let the program to run
parser.add_argument('--page_size', type=int, help='how many rows to get per page', required=True)
# The second one is an optional argument for the program to run. It means that with or without it your program should be able to work.
parser.add_argument('--num_pages', type=int, help='how many pages to get in total')
# Take the command line arguments passed in (sys.argv) and pass them through the parser.
# Then you will ned up with variables that contains page size and num pages.  
args = parser.parse_args(sys.argv[1:])
print(args)

DATASET_ID=os.environ["DATASET_ID"]
APP_TOKEN=os.environ["APP_TOKEN"]
ES_HOST=os.environ["ES_HOST"]
ES_USERNAME=os.environ["ES_USERNAME"]
ES_PASSWORD=os.environ["ES_PASSWORD"]
INDEX_NAME=os.environ["INDEX_NAME"]

if __name__ == '__main__': 
    try:
        #Using requests.put(), we are creating an index (db) first.
        resp = requests.put(f"{ES_HOST}/{INDEX_NAME}", auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
            json={
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                },
                #We are specifying the columns and define what we want the data to be.
                #However, it is not guaranteed that the data will come us clean. 
                #We will might need to clean it in the next steps.
                #If the data you're pushing to the Elasticsearch is not compatible with these definitions, 
                #you'll either won't be able to push the data to Elasticsearch in the next steps 
                #and get en error due to that or the columns will not be usable in Elasticsearch 
                "mappings": {
                    "properties": {
                        "starfire_incident_id": {"type": "keyword"},
                        "incident_datetime": {"type": "date"},
                        "alarm_box_borough": {"type": "keyword"},
                        "incident_classification": {"type": "keyword"},
                        "alarm_source_description_tx": {"type": "keyword"},
                        "dispatch_response_seconds_qy": {"type": "float"},
                        "incident_response_seconds_qy": {"type": "float"},
                        "engines_assigned_quantity": {"type": "float"},
                        "ladders_assigned_quantity": {"type": "float"},
                        "zipcode": {"type": "float"}, #This should normally be considered for keyword 
                        #but I need a numeric field for the next steps. 
                    }
                },
            }
        )
        resp.raise_for_status()
        print(resp.json())
        
    #If we send another put() request after creating an index (first put() request), the pogram will give an error and crash.
    #In order to avoid it, we use try and except here.
    #If the index is already created, it will raise an excepion and the program will not be crashed. 
    except Exception as e:
        print("Index already exists! Skipping")   
    
    client = Socrata("data.cityofnewyork.us", APP_TOKEN, timeout=10000)
    total = client.get(DATASET_ID, select="COUNT(*)")
    
    max_pagesize = math.ceil(int(total[0]["COUNT"])/int(args.page_size))
    print(max_pagesize)
    #insert for loop here.
    
    if args.num_pages is not None:
        for args.num_pages in range(1,args.num_pages):
            rows = client.get(DATASET_ID, limit=args.page_size, where="starfire_incident_id" != None and "incident_datetime" != None,  offset=(args.num_pages-1)*args.page_size)
            es_rows=[]
            
            for row in rows:
                try:
            # Convert
                    es_row = {}
                    es_row["starfire_incident_id"] = row["starfire_incident_id"]
                    es_row["incident_datetime"] = row["incident_datetime"]
                    es_row["alarm_box_borough"] = row["alarm_box_borough"]
                    es_row["incident_classification"] = row["incident_classification"]
                    es_row["alarm_source_description_tx"] = row["alarm_source_description_tx"]
                    es_row["dispatch_response_seconds_qy"] = row["dispatch_response_seconds_qy"]
                    es_row["incident_response_seconds_qy"] = row["incident_response_seconds_qy"]
                    es_row["engines_assigned_quantity"] = row["engines_assigned_quantity"]
                    es_row["ladders_assigned_quantity"] = row["ladders_assigned_quantity"]
                    es_row["zipcode"] = float(row["zipcode"]) 
            
            #print(es_row)
        
        #There might be still some bad data coming from the source
        #For instance, incident_zip might have N/A instead of numerice values.
        #In this case, the conversion will not work and the program will crash.
        #We do not want that. That's why we raise an exception here. 
                except Exception as e:
                    print (f"Error!: {e}, skipping row: {row}")
                    continue
        
                es_rows.append(es_row)
                #print(es_rows)
        
            bulk_upload_data = ""
            for line in es_rows:
                print(f'Handling row {line["starfire_incident_id"]}')
                action = '{"index": {"_index": "' + INDEX_NAME + '", "_type": "_doc", "_id": "' + line["starfire_incident_id"] + '"}}'
                data = json.dumps(line)
                bulk_upload_data += f"{action}\n"
                bulk_upload_data += f"{data}\n"

            try:
            # Upload to Elasticsearch by creating a document
                resp = requests.post(f"{ES_HOST}/_bulk",
                # We upload es_row to Elasticsearch
                            data=bulk_upload_data,auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD), headers = {"Content-Type": "application/x-ndjson"})
                resp.raise_for_status()
                print ('Done')
                #print(max_pagesize)
           
            except Exception as e:
                print(f"Failed to insert in ES: {e}") 

    else:
        for max_pagesize in range(1,max_pagesize):
            rows = client.get(DATASET_ID, limit=args.page_size, where="starfire_incident_id" != None and "incident_datetime" != None, offset=(max_pagesize-1)*args.page_size)
            es_rows=[]
                
            for row in rows:
                try:
            # Convert
                    es_row = {}
                    es_row["starfire_incident_id"] = row["starfire_incident_id"]
                    es_row["incident_datetime"] = row["incident_datetime"]
                    es_row["alarm_box_borough"] = row["alarm_box_borough"]
                    es_row["incident_classification"] = row["incident_classification"]
                    es_row["alarm_source_description_tx"] = row["alarm_source_description_tx"]
                    es_row["dispatch_response_seconds_qy"] = row["dispatch_response_seconds_qy"]
                    es_row["incident_response_seconds_qy"] = row["incident_response_seconds_qy"]
                    es_row["engines_assigned_quantity"] = row["engines_assigned_quantity"]
                    es_row["ladders_assigned_quantity"] = row["ladders_assigned_quantity"]
                    es_row["zipcode"] = float(row["zipcode"]) 
            
            #print(es_row)
        
        #There might be still some bad data coming from the source
        #For instance, incident_zip might have N/A instead of numerice values.
        #In this case, the conversion will not work and the program will crash.
        #We do not want that. That's why we raise an exception here. 
                except Exception as e:
                    print (f"Error!: {e}, skipping row: {row}")
                    continue
        
                es_rows.append(es_row)
                #print(es_rows)
        
            bulk_upload_data = ""
            for line in es_rows:
                print(f'Handling row {line["starfire_incident_id"]}')
                action = '{"index": {"_index": "' + INDEX_NAME + '", "_type": "_doc", "_id": "' + line["starfire_incident_id"] + '"}}'
                data = json.dumps(line)
                bulk_upload_data += f"{action}\n"
                bulk_upload_data += f"{data}\n"

            try:
            # Upload to Elasticsearch by creating a document
                resp = requests.post(f"{ES_HOST}/_bulk",
                # We upload es_row to Elasticsearch
                            data=bulk_upload_data,auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD), headers = {"Content-Type": "application/x-ndjson"})
                resp.raise_for_status()
                print ('Done')
                print(total)
           
            except Exception as e:
                print(f"Failed to insert in ES: {e}")
