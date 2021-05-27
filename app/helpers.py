import csv
import itertools
import json
import logging as log
import os

dirName = os.path.dirname(__file__)
log.basicConfig(level=log.DEBUG)


def getCredentials():
    with open(os.path.join(dirName, '../config/credentials.json')) as f:
        return json.load(f)

def getSchema():
    with open(os.path.join(dirName, '../config/schemas.json')) as f:
        return json.load(f)

def dictToCsv(dict, filename, headers=True, append=False):
    """Converts a dictionary of lists to a csv output"""
    #dict: the dictionary of lists to write
    #filename: the directory to write the file
    #headers: Whether to include headers as the first line in the csv
    #append: whether to overwrite or append to the file
    mode = 'w'
    if append:
        mode = 'a'
    with open(filename, mode, newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=",")
        if headers:
            writer.writerow(dict.keys())
        writer.writerows(itertools.zip_longest(*dict.values(), fillvalue=""))
            
class FailedToConnectError(Exception):
    def __init__(self, message="Failed to connect to API"):
        self.message = message
        super().__init__(self.message)
