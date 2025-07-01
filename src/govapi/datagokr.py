# -*- coding: utf-8 -*-
# 공공데이터포털 (https://www.data.go.kr/)



import os 
import sys 
from xml.dom import minidom
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ElementTree
from urllib.parse import urlparse
from dataclasses import dataclass
from time import sleep
from datetime import datetime
import subprocess
import re


import requests
import pandas as pd


from ipylib.idebug import *
from ipylib import ifile 





class DataGoKrAPIKey:

    def __init__(self):
        self.cred_path = os.environ['DATA_GO_KR_CREDENTIAL_PATH']
        self._dic = ifile.read_jsonfile(self.cred_path)

    @property
    def decode_key(self):
        if self._dic:
            return self._dic['Decoding']
        
    @property
    def encode_key(self):
        if self._dic:
            return self._dic['Encoding']
    
APIKey = DataGoKrAPIKey()


def view_xml(text):
    dom1 = minidom.parseString(text)
    new_xml = dom1.toprettyxml()
    print(new_xml)


def _write_xmlFile(filename, xml_text):
    path = os.path.abspath(__file__)
    _dir = os.path.dirname(path)
    _dir = os.path.join(_dir, '_temp_xmls')
    try:
        os.mkdir(_dir)
    except Exception as e:
        pass 
    filepath = os.path.join(_dir, filename) + '.xml'
    # print(xml_text)
    root = ET.fromstring(xml_text)
    head = root.find('header')
    # print(head)
    ElementTree(head).write(filepath, encoding='UTF-8')
    return filepath


def _handle_response(response):
    # pp.pprint(response.__dict__)
    o = urlparse(response.url)
    # print(o)
    dataName = o.path.split('/')[-1]
    d = {'dataName': dataName}

    root = ET.fromstring(response.text)
    resultCode = root.find('header/resultCode').text
    resultMsg = root.find('header/resultMsg').text
    d.update({
        'resultCode': resultCode,
        'resultMsg': resultMsg,
    })

    xml_file = _write_xmlFile(dataName, response.text)
    print({'xml_file': xml_file})

    # 바디 메타정보 뽑아내기
    if resultCode == '00':
        d.update({
            'numOfRows': int(root.find('body/numOfRows').text),
            'pageNo': int(root.find('body/pageNo').text),
            'totalCount': int(root.find('body/totalCount').text),
        })    
    else:
        logger.warning([dataName, resultMsg])
    
    # 바디 데이타 뽑아내기
    try:
        items = root.find('body/items')
        ElementTree(items).write(xml_file, encoding='UTF-8')
        df = pd.read_xml(xml_file)
    except Exception as e:
        logger.error([dataName, e])
        d.update({'data': []})
    else:
        data = df.to_dict('records')
        print(dataName, {'DataLen': len(data)})
        d.update({'data': data})
    
    os.remove(xml_file)

    return d 


def _inputTradeMonth(value):
    if value is None:
        return datetime.now().astimezone().strftime('%Y%m')
    else:
        return value




REQUEST_DATA = [
    {
        "api_name": "국토교통부_아파트매매 실거래 상세 자료",
        "url": "http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev",
        "func_name": "getRTMSDataSvcAptTradeDev",
        "params": {
            'pageNo': '1',
            'numOfRows': 1000,
            'LAWD_CD': locationCode,
            'DEAL_YMD': tradeMonth
        }
    },
    {
        "api_name": "국토교통부_아파트 전월세 자료",
        "url": "http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptRent",
        "params": {
            'pageNo': '1',
            'numOfRows': n_rows,
            'LAWD_CD': locationCode,
            'DEAL_YMD': tradeMonth
        }
    },
    {
        "api_name": "국토교통부_연립다세대 매매 실거래자료",
        "url": "http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcRHTrade",
        "params": {
            'pageNo': '1',
            'numOfRows': n_rows,
            'LAWD_CD': locationCode,
            'DEAL_YMD': tradeMonth
        }
    },
    {
        "api_name": "국토교통부_연립다세대 전월세 자료",
        "url": "http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcRHRent",
        "params": {
            'pageNo': '1',
            'numOfRows': n_rows,
            'LAWD_CD': locationCode,
            'DEAL_YMD': tradeMonth
        }
    },
    {
        "api_name": "국토교통부_아파트 분양권전매 신고 자료",
        "url": "http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcSilvTrade",
        "params": {
            'pageNo': '1',
            'numOfRows': n_rows,
            'LAWD_CD': locationCode,
            'DEAL_YMD': tradeMonth
        }
    }
]


def find_api(api_name):
    for d in REQUEST_DATA:
        if re.search(api_name, d['api_name']):
            return d 



def get_data(
    api_name:str,
    locationCode:str, 
    tradeMonth:str=None, 
    n_rows:int=1000
):
    d = find_api(api_name)
    if d:
        url = d['url']
        params = d['params']
        params.update({
            "serviceKey": APIKey.decode_key,
            "numOfRows": n_rows,
            "LAWD_CD": locationCode,
            "DEAL_YMD": _inputTradeMonth(tradeMonth),
        })
    
        try:
            response = requests.get(url, params=params)
        except requests.ConnectionError as e:
            print(f"\nERROR | {e}")

            filepath = os.path.realpath(os.environ['RUN_FILE_PATH'])
            subprocess.run([sys.executable, filepath] + sys.argv[1:])
        finally:
            return _handle_response(response)


