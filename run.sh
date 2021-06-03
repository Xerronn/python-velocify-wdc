#!/bin/bash
parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )

cd "$parent_path"

source env/bin/activate
python app/getLeads.py &
python app/getCallHistoryReport.py &
wait
python app/truncate.py
wait
python app/uploadData.py
