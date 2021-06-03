import logging as log
import os
import time
from datetime import datetime

from google.cloud import storage


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    log.info(
        "Uploading {} file to {}.".format(
            source_file_name, destination_blob_name
        )
    )

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.chunk_size = 1024 * 1024 * 10

    blob.upload_from_filename(source_file_name)

    log.info("Success!")

#generate a google service account with json keys, add them to the bucket's add data permission group
#place the resulting json file at this directory
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "config/keys.json"

start_time = time.time()
uploadTime = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')

upload_blob("angel_oak", "csv/LeadAttributes.csv", f"{uploadTime}/LeadAttributes.csv")
upload_blob("angel_oak", "csv/LeadFields.csv", f"{uploadTime}/LeadFields.csv")
upload_blob("angel_oak", "csv/LeadActionLogs.csv", f"{uploadTime}/LeadActionLogs.csv")
upload_blob("angel_oak", "csv/LeadAssignmentLogs.csv", f"{uploadTime}/LeadAssignmentLogs.csv")
upload_blob("angel_oak", "csv/LeadCreationLogs.csv", f"{uploadTime}/LeadCreationLogs.csv")
upload_blob("angel_oak", "csv/LeadDistributionLogs.csv", f"{uploadTime}/LeadDistributionLogs.csv")
upload_blob("angel_oak", "csv/LeadEmailLogs.csv", f"{uploadTime}/LeadEmailLogs.csv")
upload_blob("angel_oak", "csv/LeadStatusLogs.csv", f"{uploadTime}/LeadStatusLogs.csv")
upload_blob("angel_oak", "csv/CallHistoryReport.csv", f"{uploadTime}/CallHistoryReport.csv")

print("--- %s seconds total upload time ---" % (time.time() - start_time))
