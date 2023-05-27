# -*- coding: utf-8 -*-
import json 
import os 


import requests


from ipylib.idebug import *





@ftracer
def apikey():
    file = os.path.abspath("C:\\pypjts\\GovDataScience\\authkey.json")
    with open(file, "r") as f:
        text = f.read()
    d = json.loads(text)
    encodeKey = d['EncodingKey']
    decodeKey = d['DecodingKey']
    return encodeKey, decodeKey
    



"""국토교통부_아파트매매 실거래 상세 자료"""
@ftracer
def getRTMSDataSvcAptTradeDev(locationCode='11110', tradeMonth='202305', n_rows='1000'):
    encodeKey, decodeKey = apikey()

    url = 'http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev'
    params ={'serviceKey' : decodeKey, 'pageNo' : '1', 'numOfRows' : n_rows, 'LAWD_CD' : locationCode, 'DEAL_YMD' : tradeMonth }

    response = requests.get(url, params=params)
    return response