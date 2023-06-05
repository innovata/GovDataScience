# -*- coding: utf-8 -*-
import json 
import os 
from xml.dom import minidom
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ElementTree
from urllib.parse import urlparse
from dataclasses import dataclass


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
    path = os.path.dirname(path)
    path = os.path.join(path, '_temp_xmls', filename)
    return path


def _handle_response(response):
    root = ET.fromstring(response.text)
    resultCode = root.find('header/resultCode').text
    if resultCode == '00': 
        cnt = root.find('body/totalCount').text
        if len(cnt) == 0: 
            view_xml(response.text)
            data = []
        else:
            o = urlparse(response.url)
            filename = o.path.split('/')[-1]
            fpath = _build_xmlFilepath(filename)

            items = root.find('body/items')
            ElementTree(items).write(fpath, encoding='UTF-8')
            df = pd.read_xml('output.xml')
            data = df.to_dict('records')

        return {
            'dataName': filename,
            'resultCode': root.find('header/resultCode').text,
            'resultMsg': root.find('header/resultMsg').text,
            'numOfRows': root.find('body/numOfRows').text,
            'pageNo': root.find('body/pageNo').text,
            'totalCount': root.find('body/totalCount').text,
            'data': data,
        }
    else:
        view_xml(response.text)



"""국토교통부_아파트매매 실거래 상세 자료"""
@ftracer
def getRTMSDataSvcAptTradeDev(locationCode='11110', tradeMonth='202305', n_rows='1000'):
    url = 'http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev'
    params ={'serviceKey' : APIKey.DataGoKr.DecodingKey, 'pageNo' : '1', 'numOfRows' : n_rows, 'LAWD_CD' : locationCode, 'DEAL_YMD' : tradeMonth }
    response = requests.get(url, params=params)

    return _handle_response(response)
    



"""국토교통부_아파트 전월세 자료"""
# @ftracer
def getRTMSDataSvcAptRent(locationCode='11110', tradeMonth='202305'):
    url = 'http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptRent'
    params ={'serviceKey' : APIKey.DataGoKr.DecodingKey, 'LAWD_CD' : locationCode, 'DEAL_YMD' : tradeMonth}
    response = requests.get(url, params=params)
    
    return _handle_response(response)


"""국토교통부_연립다세대 매매 실거래자료"""
@ftracer
def getRTMSDataSvcRHTrade(locationCode='11110', tradeMonth='202305'):
    url = 'http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcRHTrade'
    params ={'serviceKey' : APIKey.DataGoKr.DecodingKey, 'LAWD_CD' : locationCode, 'DEAL_YMD' : tradeMonth }
    response = requests.get(url, params=params)

    return _handle_response(response)


"""국토교통부_연립다세대 전월세 자료"""
@ftracer
def getRTMSDataSvcRHRent(locationCode='11110', tradeMonth='202305'):
    url = 'http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcRHRent'
    params ={'serviceKey' : APIKey.DataGoKr.DecodingKey, 'LAWD_CD' : locationCode, 'DEAL_YMD' : tradeMonth }
    response = requests.get(url, params=params)

    return _handle_response(response)



"""국토교통부_아파트 분양권전매 신고 자료"""
@ftracer
def getRTMSDataSvcSilvTrade(locationCode='11110', tradeMonth='202305'):
    url = 'http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcSilvTrade'
    params ={'serviceKey' : APIKey.DataGoKr.DecodingKey, 'LAWD_CD' : locationCode, 'DEAL_YMD' : tradeMonth }
    response = requests.get(url, params=params)

    return _handle_response(response)

