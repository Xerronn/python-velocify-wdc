import os
import json
from google.cloud import storage
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
import dateutil.relativedelta

#for testing
import time

#generate a google service account with json keys, add them to the bucket add data permission group
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "keys.json"

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
    print(f"Getting month {i} data...")
    
    attempt = 0
    while(True):
        try:
            start_time_query = time.time()
            query = requests.get(f"https://service.prod.velocify.com/ClientService.asmx/getLeads?username={credentials['username']}&password={credentials['password']}&from="
            f"{(yearAgo + dateutil.relativedelta.relativedelta(months=i, days=-1)).strftime('%m/%d/%Y')}&to={(yearAgo + dateutil.relativedelta.relativedelta(months=i+1)).strftime('%m/%d/%Y')}")
            print(f"--- {time.time() - start_time_query} seconds for query on month {i} ---")
            break
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except:
            print("Connection error, retrying...")
            if attempt > 3:
                raise FailedToConnectError
            else:
                attempt += 1
    
    start_time_parsing = time.time()
    data = ET.fromstring(query.content)
    print(f"--- {time.time() - start_time_parsing} seconds for parsing on month {i} ---")

    start_time_processing = time.time()
    for lead in data:
        row = {}
        for key in lead.attrib.keys():
            row[key] = lead.attrib[key]
        #give one version of the ID that is int and one that is string
        row["EncompassId"] = lead.attrib["Id"]

        try:
            #campaign table
            campaign = lead.find("Campaign")
            for key in campaign.attrib.keys():
                row[key] = campaign.attrib[key]
        except:
            pass
        
        try:
            #status table
            status = lead.find("Status")
            for key in status.attrib.keys():
                row[key] = status.attrib[key]
        except:
            pass
        
        try:
            #agent table
            agent = lead.find("Agent")
            for key in agent.attrib.keys():
                row[key] = agent.attrib[key]
        except:
            pass

        #dataframes are slow but they are fine for this instance.
        start_time_df = time.time()
        leadsDF = leadsDF.append(row, ignore_index=True)
        print(f"--- {time.time() - start_time_df} seconds for df append on month {i} ---")
    
    if i == 0:
        leadsDF.to_csv("LeadsData.csv")
    else:
        leadsDF.to_csv("LeadsData.csv", mode='a', header=False)

    print(f"--- {time.time() - start_time_processing} seconds for processing on month {i} ---")

upload_blob("angel_oak", "LeadsData.csv", f"LeadsTest{datetime.now().strftime('%d-%m-%Y_%H:%M:%S')}.csv")

print("--- %s seconds ---" % (time.time() - start_time))