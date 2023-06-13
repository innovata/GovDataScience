# -*- coding: utf-8 -*-
import json 
import os 
from xml.dom import minidom
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ElementTree
from urllib.parse import urlparse
from dataclasses import dataclass
from time import sleep
from datetime import datetime


import requests
import pandas as pd


from ipylib.idebug import *


from govdatascience.openapi import APIKey





def view_xml(text):
    dom1 = minidom.parseString(text)
    new_xml = dom1.toprettyxml()
    print(new_xml)


def _build_xmlFilepath(filename):
    path = os.path.abspath(__file__)
    _dir = os.path.dirname(path)
    _dir = os.path.join(_dir, '_temp_xmls')
    try:
        os.mkdir(_dir)
    except Exception as e:
        pass 
    filepath = os.path.join(_dir, filename)
    return filepath + '.xml'


def _handle_response(response):
    # pp.pprint(response.__dict__)
    root = ET.fromstring(response.text)

    d = {
        'resultCode': root.find('header/resultCode').text,
        'resultMsg': root.find('header/resultMsg').text,
        'numOfRows': int(root.find('body/numOfRows').text),
        'pageNo': int(root.find('body/pageNo').text),
        'totalCount': int(root.find('body/totalCount').text),
    }
    
    o = urlparse(response.url)
    # print(o)
    dataName = o.path.split('/')[-1]
    xml_file = _build_xmlFilepath(dataName)
    d.update({'dataName': dataName})
    # print({'xml_file': xml_file})

    items = root.find('body/items')
    ElementTree(items).write(xml_file, encoding='UTF-8')

    try:
        df = pd.read_xml(xml_file)
        data = df.to_dict('records')
    except Exception as e:
        logger.warning([e, d])
        data = []
    print(dataName, {'DataLen': len(data)})
    d.update({'data': data})
    
    os.remove(xml_file)

    return d 


def _inputTradeMonth(value):
    if value is None:
        return datetime.now().astimezone().strftime('%Y%m')
    else:
        return value



"""국토교통부_아파트매매 실거래 상세 자료"""
@ftracer
def getRTMSDataSvcAptTradeDev(locationCode, tradeMonth=None, n_rows='1000'):
    url = 'http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev'
    tradeMonth = _inputTradeMonth(tradeMonth)
    params ={'serviceKey' : APIKey.DataGoKr.DecodingKey, 'pageNo' : '1', 'numOfRows' : n_rows, 'LAWD_CD' : locationCode, 'DEAL_YMD' : tradeMonth }
    response = requests.get(url, params=params)

    return _handle_response(response)
    



"""국토교통부_아파트 전월세 자료"""
# @ftracer
def getRTMSDataSvcAptRent(locationCode, tradeMonth=None):
    url = 'http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptRent'
    tradeMonth = _inputTradeMonth(tradeMonth)
    params ={'serviceKey' : APIKey.DataGoKr.DecodingKey, 'LAWD_CD' : locationCode, 'DEAL_YMD' : tradeMonth}
    response = requests.get(url, params=params)
    
    return _handle_response(response)


"""국토교통부_연립다세대 매매 실거래자료"""
@ftracer
def getRTMSDataSvcRHTrade(locationCode, tradeMonth=None):
    url = 'http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcRHTrade'
    tradeMonth = _inputTradeMonth(tradeMonth)
    params ={'serviceKey' : APIKey.DataGoKr.DecodingKey, 'LAWD_CD' : locationCode, 'DEAL_YMD' : tradeMonth }
    response = requests.get(url, params=params)

    return _handle_response(response)


"""국토교통부_연립다세대 전월세 자료"""
@ftracer
def getRTMSDataSvcRHRent(locationCode, tradeMonth=None):
    url = 'http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcRHRent'
    tradeMonth = _inputTradeMonth(tradeMonth)
    params ={'serviceKey' : APIKey.DataGoKr.DecodingKey, 'LAWD_CD' : locationCode, 'DEAL_YMD' : tradeMonth }
    response = requests.get(url, params=params)

    return _handle_response(response)



"""국토교통부_아파트 분양권전매 신고 자료"""
@ftracer
def getRTMSDataSvcSilvTrade(locationCode, tradeMonth=None):
    url = 'http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcSilvTrade'
    tradeMonth = _inputTradeMonth(tradeMonth)
    params ={'serviceKey' : APIKey.DataGoKr.DecodingKey, 'LAWD_CD' : locationCode, 'DEAL_YMD' : tradeMonth }
    response = requests.get(url, params=params)

    return _handle_response(response)

