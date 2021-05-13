from uploadData import getCredentials, dictToCsv, FailedToConnectError
from requests_futures.sessions import FuturesSession
from concurrent.futures import as_completed
import xml.etree.ElementTree as ET
from datetime import datetime
import dateutil.relativedelta
import logging as log
import time
import os

log.basicConfig(level=log.DEBUG)

start_time = time.time()

#one day in the future just to get everything
today = datetime.now() + dateutil.relativedelta.relativedelta(days=1)
yearAgo = today + dateutil.relativedelta.relativedelta(years=-1)
filelocked = False
credentials = getCredentials()

pag=[]
for i in range(365):
    pag.append(f"https://service.prod.velocify.com/ClientService.asmx/GetCallHistoryReport?username={credentials['username']}&password={credentials['password']}&startDate="
        f"{(yearAgo + dateutil.relativedelta.relativedelta(days = i)).strftime('%m/%d/%Y')}&endDate={(yearAgo + dateutil.relativedelta.relativedelta(days=i+1)).strftime('%m/%d/%Y')}"
    )

#erase the file contents
open("csv/CallHistoryReport.csv", "w").close()
with FuturesSession() as session:
    futures = [session.get(req) for req in pag]
    for future in as_completed(futures):
        callData = {}
        response = future.result()

        data = ET.fromstring(response.content)

        callData = {}
        for call in data:
            for key in call.attrib.keys():
                callData.setdefault(key,[]).append(call.attrib[key])
        
        #if file is empty, add headers, else just append
        while(True):
            if not filelocked:
                #to prevent multithreading shenanigans
                filelocked = True
                if os.stat("csv/CallHistoryReport.csv").st_size == 0:
                    dictToCsv(callData, "csv/CallHistoryReport.csv")
                else:
                    dictToCsv(callData, "csv/CallHistoryReport.csv", headers=False, append=True)
                filelocked = False
                break
        

print("--- %s seconds total execution time ---" % (time.time() - start_time))