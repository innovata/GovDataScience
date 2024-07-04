# -*- coding: utf-8 -*-
import json 
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


import requests
import pandas as pd


from ipylib.idebug import *


from govdatascience.openapi import APIKey



def get_apikey():

    # 1차 시도: 개인API인증키를 직접 입력할 경우
    try:
        return os.environ['DATAGOKR_DECODE_KEY']
    except Exception as e:
        logger.error(e)
        # 2차 시도: 개인API인증키가 있는 파일을 읽어들이는 경우
        try:
            filepath = os.environ['DATAGOKR_DECODE_KEY_FILEPATH']
        except Exception as e:
            logger.error(e)
            raise
        else:
            try:
                with open(filepath, "r") as f:
                    text = f.read()
                    f.close()
                    d = json.loads(text)
                return d['Decoding']
            except Exception as e:
                logger.error(e)
                raise



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



def __reqGet__(url, params):
    params.update({'serviceKey': get_apikey()})
    try:
        response = requests.get(url, params=params)
    except requests.ConnectionError as e:
        logger.error([e, url, params])

        filepath = os.path.realpath(os.environ['RUN_FILE_PATH'])
        subprocess.run([sys.executable, filepath] + sys.argv[1:])
    finally:
        return response



"""국토교통부_아파트매매 실거래 상세 자료"""
@ftracer
def getRTMSDataSvcAptTradeDev(locationCode, tradeMonth=None, n_rows='1000'):
    url = 'http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev'
    tradeMonth = _inputTradeMonth(tradeMonth)
    params ={'pageNo' : '1', 'numOfRows' : n_rows, 'LAWD_CD' : locationCode, 'DEAL_YMD' : tradeMonth }
    res = __reqGet__(url, params)
    return _handle_response(res)


"""국토교통부_아파트 전월세 자료"""
# @ftracer
def getRTMSDataSvcAptRent(locationCode, tradeMonth=None):
    url = 'http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptRent'
    tradeMonth = _inputTradeMonth(tradeMonth)
    params ={'LAWD_CD' : locationCode, 'DEAL_YMD' : tradeMonth}
    res = __reqGet__(url, params)
    return _handle_response(res)


"""국토교통부_연립다세대 매매 실거래자료"""
@ftracer
def getRTMSDataSvcRHTrade(locationCode, tradeMonth=None):
    url = 'http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcRHTrade'
    tradeMonth = _inputTradeMonth(tradeMonth)
    params ={'LAWD_CD' : locationCode, 'DEAL_YMD' : tradeMonth }
    res = __reqGet__(url, params)
    return _handle_response(res)


"""국토교통부_연립다세대 전월세 자료"""
@ftracer
def getRTMSDataSvcRHRent(locationCode, tradeMonth=None):
    url = 'http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcRHRent'
    tradeMonth = _inputTradeMonth(tradeMonth)
    params ={'LAWD_CD' : locationCode, 'DEAL_YMD' : tradeMonth }
    res = __reqGet__(url, params)
    return _handle_response(res)


"""국토교통부_아파트 분양권전매 신고 자료"""
@ftracer
def getRTMSDataSvcSilvTrade(locationCode, tradeMonth=None):
    url = 'http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcSilvTrade'
    tradeMonth = _inputTradeMonth(tradeMonth)
    params ={'LAWD_CD' : locationCode, 'DEAL_YMD' : tradeMonth }
    res = __reqGet__(url, params)
    return _handle_response(res)

