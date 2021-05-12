import os
import json
from google.cloud import storage
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
import dateutil.relativedelta
import logging as log

#for testing
import time

#generate a google service account with json keys, add them to the bucket's add data permission group
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "keys.json"
log.basicConfig(level=log.DEBUG)

with open('credentials.json') as f:
    credentials = json.load(f)

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )

class FailedToConnectError(Exception):
    def __init__(self, salary, message="Failed to connect to API"):
        self.salary = salary
        self.message = message
        super().__init__(self.message)

start_time = time.time()

#one day in the future just to get everything
today = datetime.now() + dateutil.relativedelta.relativedelta(days=1)

yearAgo = today + dateutil.relativedelta.relativedelta(years=-1)

#paginate through one month intervals until reaching one year
for i in range(12):
    leadsDF = pd.DataFrame({"Id":pd.Series([], dtype=int)})
    
    
    attempt = 0
    while(True):
        try:
            log.info(f"Getting month {i} data...")
            start_time_query = time.time()

            query = requests.get(f"https://service.prod.velocify.com/ClientService.asmx/getLeads?username={credentials['username']}&password={credentials['password']}&from="
            f"{(yearAgo + dateutil.relativedelta.relativedelta(months=i, days=-1)).strftime('%m/%d/%Y')}&to={(yearAgo + dateutil.relativedelta.relativedelta(months=i+1)).strftime('%m/%d/%Y')}")

            log.info(f"--- {time.time() - start_time_query} seconds for query on month {i} ---")
            break
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except:
            log.warning("Connection error, retrying...")
            if attempt > 3:
                raise FailedToConnectError
            else:
                attempt += 1
    
    data = ET.fromstring(query.content)
    
    start_time_processing = time.time()
    leadAttribs = {}
    leadFields = {}
    for lead in data:
        #start of lead attributes table
        for key in lead.attrib.keys():
            leadAttribs.setdefault(key,[]).append(lead.attrib[key])
        #give one version of the ID that is int and one that is string
        leadAttribs.setdefault("EncompassId", []).append(lead.attrib["Id"])

        try:
            #campaign table
            campaign = lead.find("Campaign")
            for key in campaign.attrib.keys():
                leadAttribs.setdefault(key, []).append(campaign.attrib[key])
        except:
            pass
        
        try:
            #status table
            status = lead.find("Status")
            for key in status.attrib.keys():
                leadAttribs.setdefault(key, []).append(status.attrib[key])
        except:
            pass
        
        try:
            #agent table
            agent = lead.find("Agent")
            for key in agent.attrib.keys():
                leadAttribs.setdefault(key, []).append(agent.attrib[key])
        except:
            pass
        #End of lead attributes table

        #start of lead fields table
        leadFields.setdefault("LeadId",[]).append(lead.attrib["Id"])
        try:
            for field in lead.iter('Field'):
                leadFields.setdefault("FieldId", []).append(field.attrib["FieldId"])
                leadFields.setdefault(field.attrib["FieldTitle"], []).append(field.attrib["Value"])
        except:
            pass

    log.info(f"--- {time.time() - start_time_processing} seconds for processing on month {i} ---")        
    
    #dataframes are slow but they are fine for this instance.
    start_time_df1 = time.time()
    leadAttribsDF = pd.DataFrame.from_dict(leadAttribs,orient='index').transpose()
    log.info(f"--- {time.time() - start_time_df1} seconds for df creation on month {i} ---")   
    start_time_df2 = time.time()
    leadFieldsDF = pd.DataFrame.from_dict(leadFields,orient='index').transpose()
    log.info(f"--- {time.time() - start_time_df2} seconds for df creation on month {i} ---")     
    
    if i == 0:
        leadAttribsDF.to_csv("LeadAttributes.csv")
        leadFieldsDF.to_csv("LeadFields.csv")
    else:
        leadAttribsDF.to_csv("LeadAttributes.csv", mode='a', header=False)
        leadFieldsDF.to_csv("LeadFields.csv", mode='a', header=False)

uploadTime = datetime.now().strftime('%d-%m-%Y_%H:%M:%S')
upload_blob("angel_oak", "LeadAttributes.csv", f"{uploadTime}/LeadAttributes.csv")
upload_blob("angel_oak", "LeadFields.csv", f"{uploadTime}/LeadFields.csv")

print("--- %s seconds ---" % (time.time() - start_time))