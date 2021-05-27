#!/bin/bash
source env/Scripts/activate
python app/getLeads.py &
python app/getCallHistoryReport.py &
wait
python app/truncate.py
wait
python app/uploadData.py