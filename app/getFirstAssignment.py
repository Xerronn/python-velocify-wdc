import time
import xml.etree.ElementTree as ET
import math
import dateutil.relativedelta
import requests
import logging as log

from helpers import FailedToConnectError, dictToCsv, getCredentials, getSchema

log.basicConfig(level=log.DEBUG)
credentials = getCredentials()
schema = getSchema()

start_time = time.time()


attempt = 0
while(True):
    try:
        log.info("Getting first Assignment data...")
        start_time_query = time.time()

        req = requests.get("https://service.prod.velocify.com/ClientService.asmx/GetReportResultsWithoutFilters"
            f"?username={credentials['username']}&password={credentials['password']}&&reportId=216")
        
        break
    except KeyboardInterrupt:
        raise KeyboardInterrupt
    except:
        log.warning("Connection error, retrying...")
        if attempt > 3:
            raise FailedToConnectError
        else:
            attempt += 1

data = ET.fromstring(req.content)

start_time_processing = time.time()

#objects to store the data in memory before writing it to file.
firstAssignment = {}

for assignment in data:
    for col in schema["firstAssignment"]:
        if assignment.find(col) is None:
            firstAssignment.setdefault(col.split("_")[-1],[]).append("")
        else:
            firstAssignment.setdefault(col.split("_")[-1],[]).append(assignment.find(col).text)


        
    
start_time_csv = time.time()
dictToCsv(firstAssignment, "csv/FirstAssignment.csv")
log.info(f"--- {time.time() - start_time_csv} seconds for csv writing---")

log.info(f"--- {time.time() - start_time} seconds for all first assignment data ---")