#!/bin/bash
source env/Scripts/activate
python app/getLeads.py &
python app/getCallHistoryReport.py &
wait
python app/truncate.py
wait
python app/uploadData.py
read -n 1 -s -r -p "Finished! Press any key to end"