import sys
import os
from importlib import reload 
import json 


import requests
import pandas as pd



sys.path.append(os.path.join("C:\\pypjts", 'GovDataScience', 'src'))
for path in sorted(sys.path): print(path)


from govdatascience.openapi import datagokr, sgis, bok
from govdatascience.dataengine import database, datamodels, metadata, schmodels
from govdatascience.dataengine.database import db
from govdatascience import realestate, ecos


def reload_all():
    modules =[
        database, metadata, datamodels, schmodels,
        datagokr, sgis, bok, 
        realestate, ecos,
    ]
    for m in modules: reload(m)