import sys
import os
from importlib import reload 
import json 

sys.path.append(os.path.join("C:\\pypjts", 'GovDataScience', 'src'))
for path in sorted(sys.path): print(path)


import requests
import pandas as pd



from govdatascience import openapi, realstate
from govdatascience.dataengine import database, datamodels, metadata

def reload_all():
    modules =[
        database, metadata, datamodels,
        openapi, realstate,
    ]
    for m in modules: reload(m)