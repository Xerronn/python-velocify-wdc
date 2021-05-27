import time
import xml.etree.ElementTree as ET
from datetime import datetime
import dateutil.relativedelta
import requests
import logging as log

from helpers import FailedToConnectError, dictToCsv, getCredentials, getSchema

log.basicConfig(level=log.DEBUG)
credentials = getCredentials()
schema = getSchema()

start_time = time.time()

#one day in the future just to get everything
today = datetime.now() + dateutil.relativedelta.relativedelta(days=1)
yearAgo = today + dateutil.relativedelta.relativedelta(years=-1)

#paginate through one month intervals until reaching one year
leadSession = requests.Session()
for i in range(12):
    attempt = 0
    while(True):
        try:
            log.info(f"Getting month {(yearAgo + dateutil.relativedelta.relativedelta(months=i, days=-1)).strftime('%m/%d/%Y')} data...")
            start_time_query = time.time()

            leadSession = requests.get(f"https://service.prod.velocify.com/ClientService.asmx/getLeads?username={credentials['username']}&password={credentials['password']}&from="
            f"{(yearAgo + dateutil.relativedelta.relativedelta(months=i, days=1)).strftime('%m/%d/%Y')}&to={(yearAgo + dateutil.relativedelta.relativedelta(months=i+1)).strftime('%m/%d/%Y')}")

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
    
    data = ET.fromstring(leadSession.content)
    
    start_time_processing = time.time()

    #objects to store the data in memory before writing it to file.
    leadAttribs = {}
    leadFields = {}
    leadFields["LeadId"] = []
    for col in schema["leadFields"]:
        leadFields[col] = []
    leadActionLogs = {}
    leadAssignmentLogs = {}
    leadCreationLogs = {}
    leadDistributionLogs = {}
    leadEmailLogs = {}
    leadStatusLogs = {}
    
    for lead in data:
        ################################
        #start of lead attributes table
        for key in sorted(schema["leadAttributes"]):
            leadAttribs.setdefault(key, []).append(lead.attrib.get(key, ""))
        #give one version of the ID that is int and one that is string
        leadAttribs.setdefault("EncompassId", []).append(lead.attrib["Id"])

        try:
            #campaign table
            campaign = lead.find("Campaign")
            for key in sorted(schema["leadCampaign"]):
                leadAttribs.setdefault(key, []).append(campaign.attrib.get(key, ""))
        except:
            pass
        
        try:
            #status table
            status = lead.find("Status")
            for key in sorted(schema["leadStatus"]):
                leadAttribs.setdefault(key, []).append(status.attrib.get(key, ""))
        except:
            pass
        
        try:
            #agent table
            agent = lead.find("Agent")
            for key in sorted(schema["leadAgent"]):
                leadAttribs.setdefault(key, []).append(agent.attrib.get(key, ""))
        except:
            pass
        #End of lead attributes table
        ##############################

        #############################
        #start of lead fields table
        #foreign key to field attributes table
        leadFields.setdefault("LeadId",[]).append(lead.attrib["Id"])

        #this array is defined to hold the attributes that do exist and their values, so we can fill in the
        #spots for missing values with blanks
        currentLeadFields = {}
        for field in lead.iter('Field'):
            if field.attrib["FieldTitle"] in sorted(schema["leadFields"]):
                currentLeadFields[field.attrib["FieldTitle"]] = field.attrib["Value"]
        
        #fill in fields that don't exist as blanks.
        for key in sorted(schema["leadFields"]):
            if key in sorted(currentLeadFields.keys()):
                leadFields[key].append(currentLeadFields[key])
            else:
                leadFields[key].append("")
        #end of the lead fields table
        ##############################

        ##############################
        #start of the log tables
        logs = lead.find("Logs")

        try:
            #status logs table
            statusLogs = logs.find("StatusLog")
            for l in statusLogs:
                leadStatusLogs.setdefault("LeadId", []).append(lead.attrib["Id"])
                for key in sorted(schema["statusLog"]):
                    leadStatusLogs.setdefault(key, []).append(l.attrib.get(key, ""))
        except:
            pass

        try:
            #action logs table
            actionLogs = logs.find("ActionLog")
            for l in actionLogs:
                leadActionLogs.setdefault("LeadId", []).append(lead.attrib["Id"])
                for key in sorted(schema["actionLog"]):
                    leadActionLogs.setdefault(key, []).append(l.attrib.get(key, ""))
        except:
            pass

        try:
            #email logs table
            emailLogs = logs.find("EmailLog")
            for l in emailLogs:
                leadEmailLogs.setdefault("LeadId", []).append(lead.attrib["Id"])
                for key in sorted(schema["emailLog"]):
                    leadEmailLogs.setdefault(key, []).append(l.attrib.get(key, ""))
        except:
            pass

        try:
            #distribution logs table
            distributionLogs = logs.find("DistributionLog")
            for l in distributionLogs:
                leadDistributionLogs.setdefault("LeadId", []).append(lead.attrib["Id"])
                for key in sorted(schema["distributionLog"]):
                    leadDistributionLogs.setdefault(key, []).append(l.attrib.get(key, ""))
        except:
            pass

        try:
            #creation logs table
            #no loop for this one. There is only one creation log
            creationLog = logs.find("CreationLog")
            leadCreationLogs.setdefault("LeadId", []).append(lead.attrib["Id"])
            for key in sorted(schema["creationLog"]):
                leadCreationLogs.setdefault(key, []).append(creationLog.attrib.get(key, ""))
        except:
            pass

        try:
            #assignment logs table
            assignmentLogs = logs.find("AssignmentLog")
            for l in assignmentLogs:
                leadAssignmentLogs.setdefault("LeadId", []).append(lead.attrib["Id"])
                for key in sorted(schema["assignmentLog"]):
                    leadAssignmentLogs.setdefault(key, []).append(l.attrib.get(key, ""))
        except:
            pass

    log.info(f"--- {time.time() - start_time_processing} seconds for processing on month {i} ---")        
    
    start_time_csv = time.time()
    if i == 0:
        dictToCsv(leadAttribs, "csv/LeadAttributes.csv")
        dictToCsv(leadFields, "csv/LeadFields.csv")
        dictToCsv(leadStatusLogs, "csv/LeadStatusLogs.csv")
        dictToCsv(leadActionLogs, "csv/LeadActionLogs.csv")
        dictToCsv(leadEmailLogs, "csv/LeadEmailLogs.csv")
        dictToCsv(leadDistributionLogs, "csv/LeadDistributionLogs.csv")
        dictToCsv(leadCreationLogs, "csv/LeadCreationLogs.csv")
        dictToCsv(leadAssignmentLogs, "csv/LeadAssignmentLogs.csv")
    else:
        dictToCsv(leadAttribs, "csv/LeadAttributes.csv", headers=False, append=True)
        dictToCsv(leadFields, "csv/LeadFields.csv", headers=False, append=True)
        dictToCsv(leadStatusLogs, "csv/LeadStatusLogs.csv", headers=False, append=True)
        dictToCsv(leadActionLogs, "csv/LeadActionLogs.csv", headers=False, append=True)
        dictToCsv(leadEmailLogs, "csv/LeadEmailLogs.csv", headers=False, append=True)
        dictToCsv(leadDistributionLogs, "csv/LeadDistributionLogs.csv", headers=False, append=True)
        dictToCsv(leadCreationLogs, "csv/LeadCreationLogs.csv", headers=False, append=True)
        dictToCsv(leadAssignmentLogs, "csv/LeadAssignmentLogs.csv", headers=False, append=True)
    log.info(f"--- {time.time() - start_time_csv} seconds for csv writing on month {i} ---")

log.info(f"--- {time.time() - start_time} seconds for leads data over 12 months ---")