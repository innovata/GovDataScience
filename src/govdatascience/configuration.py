import os 
import sys 



import json 
import os 


from ipylib.idebug import *



def read_credentials():
    try:
        file = os.path.join('D:\\pypjts', 'GovDataScience', 'credentials.json')
        with open(file, "r") as f:
            text = f.read()
        d = json.loads(text)
        return d
    except Exception as e:
        logger.error(e)
        return {}






DATABASE_NAME = 'GovernmentData'

PROJECT_PATH = os.path.join('D:\pypjts', 'GovDataScience')
SCHEMA_FILE_PATH = os.path.join(PROJECT_PATH, 'Data', 'SchemaCSV')