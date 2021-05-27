import logging as log
import os
import time
import xml.etree.ElementTree as ET
from concurrent.futures import as_completed
from datetime import datetime

import dateutil.relativedelta
from requests_futures.sessions import FuturesSession

from helpers import FailedToConnectError, dictToCsv, getCredentials, getSchema

log.basicConfig(level=log.DEBUG)
credentials = getCredentials()
schema = getSchema()

start_time = time.time()

#one day in the future just to make sure we get everything
today = datetime.now() + dateutil.relativedelta.relativedelta(days=1)
pastDate = today.replace(day=1) + dateutil.relativedelta.relativedelta(months=-13)
numDays = (today - pastDate).days

#ensure the days are always odd for pagination pairing(start at 0)
if (numDays % 2) == 0:
    numDays = numDays + 1

pag = []
for i in range(0, numDays, 2):
    pag.append(f"https://service.prod.velocify.com/ClientService.asmx/GetCallHistoryReport?username={credentials['username']}&password={credentials['password']}&startDate="
        f"{(pastDate + dateutil.relativedelta.relativedelta(days = i)).strftime('%m/%d/%Y')}&endDate={(pastDate + dateutil.relativedelta.relativedelta(days=i + 1)).strftime('%m/%d/%Y')}"
    )

#erase the file contents
open("csv/CallHistoryReport.csv", "w").close()

#make sure that the thread prevention file isn't still written for some reason
if os.path.exists("getCallHistoryReport.lock"):
    os.remove("getCallHistoryReport.lock")

#async call for requests
with FuturesSession() as session:
    futures = [session.get(req) for req in pag]
    for future in as_completed(futures):
        callData = {}
        response = future.result()

        data = ET.fromstring(response.content)

        callData = {}
        for call in data:
            for key in sorted(schema["callHistoryReport"]):
                callData.setdefault(key,[]).append(call.attrib.get(key, ""))
            
        #solution to multithreading file writing. I check to see if a lock file exists, 
        # if it doesn't you make one then can edit the file. if it does, wait until there isn't
        while(True):
            if not os.path.exists("getCallHistoryReport.lock"):
                open("getCallHistoryReport.lock", 'w').close()

                #if file is empty, add headers, else just append
                if os.stat("csv/CallHistoryReport.csv").st_size == 0:
                    dictToCsv(callData, "csv/CallHistoryReport.csv")
                else:
                    dictToCsv(callData, "csv/CallHistoryReport.csv", headers=False, append=True)
                os.remove("getCallHistoryReport.lock")
                break
            else:
                time.sleep(0.1)
        

print("--- %s seconds total execution time ---" % (time.time() - start_time))
