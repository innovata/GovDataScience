# -*- coding: utf-8 -*-
import os 
from urllib.parse import urlparse


import requests


from ipylib.idebug import *


from govdatascience.dataengine import datamodels




DATA_PATH = os.path.join('c:\pypjts', 'GovDataScience', 'Data')


"""웹저장소로부터 데이터파일(CSV, Excel, ZIP 등등)을 다운로드"""
def download(url, _dir, method='GET'):
    o = urlparse(url)
    print(o)
    filename = os.path.basename(o.path)

    res = requests.request(method, url)
    print(res)
    # pp.pprint(res.__dict__)
    # print(res.content.strip())
    
    if res.status_code == 200:
        os.makedirs(_dir, exist_ok=True)
        filepath = os.path.join(_dir, filename)
        with open(filepath, 'wb') as f:
            f.write(res.content.strip())

        logger.info('Downloaded. ' + filepath)
    else: 
        pp.pprint(res.__dict__)
        raise 


"""웹저장소에 데이터파일(CSV, Excel, ZIP 등등)을 업로드"""
def upload():
    logger.info("Complete Uploading")



def initialize_db():
    url = 'https://raw.githubusercontent.com/innovata/GovDataScience/main/Data/법정동코드.txt'
    download(url, DATA_PATH)
    datamodels.LocationCode().__create__()


    # AptTradeRealContract


    logger.info('기본DB셋업완료')


def backup_db():
    logger.info('DB데이터 백업완료')