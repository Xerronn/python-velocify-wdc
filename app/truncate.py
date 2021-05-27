import snowflake.connector
from helpers import getCredentials, dirName
import os
import time
import logging as log

credentials = getCredentials()
log.basicConfig(level=log.INFO)
fileName = os.path.join(dirName, '../csv')

csvFiles = []
for path, directory, files in os.walk(fileName):
    csvFiles = files


ctx = snowflake.connector.connect(
        user = credentials["snowflake_username"],
        password = credentials["snowflake_password"],
        account = credentials["snowflake_account"]
        )

try:
    for file in csvFiles:
        ctx.cursor().execute(f"TRUNCATE TABLE ANGELOAK.VELOCIFY.{file[:-4].upper()}")
finally:
    ctx.close()
    #give the truncates time to finish
    time.sleep(30)
