# -*- coding: utf-8 -*-
import itertools
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ElementTree
import re 
from time import sleep
from datetime import datetime, timedelta
import sys 


import pandas as pd


from ipylib.idebug import *


from govdatascience.openapi import datagokr
from govdatascience.dataengine import datamodels, schmodels








"""국토교통부_부동산거래자료 통합수집기"""
def collect_all_RealState():
    data_types = [
        '아파트매매 실거래',
        '아파트 전월세',
        '연립다세대 매매 실거래',
        '연립다세대 전월세'
    ]
    for data_type in data_types:
        collect_one_RealState(data_type)

    logger.info('부동산데이터수집완료')
    sys.exit(0)


def _get_openapi_function(data_type):
    _map = {
        '아파트.*매매.*실거래': 'getRTMSDataSvcAptTradeDev',
        '아파트.*전월세': 'getRTMSDataSvcAptRent',
        '연립.*매매.*실거래': 'getRTMSDataSvcRHTrade',
        '연립.*전월세': 'getRTMSDataSvcRHRent',
        '아파트.*분양권.*전매': 'getRTMSDataSvcSilvTrade',
    }
    for pat, dataName in _map.items():
        m = re.search(pat, data_type)
        if m is None: pass 
        else: 
            return getattr(datagokr, dataName), dataName

def _get_datamodel(data_type):
    _map = {
        '아파트.*매매.*실거래': datamodels.AptTradeRealContract,
        '아파트.*전월세': datamodels.AptRent,
        '연립.*매매.*실거래': datamodels.VillaTradeRealContract,
        '연립.*전월세': datamodels.VillaRent,
    }
    for pat, model in _map.items():
        m = re.search(pat, data_type)
        if m is None: pass 
        else: return model()


def collect_one_RealState(data_type):
    req_func, dataName = _get_openapi_function(data_type)
    if req_func is None: raise 
    print({'OpenAPIFunction': dataName})

    model1 = _get_datamodel(data_type)
    if model1 is None: raise
    print({'DataModel': model1})

    # 신청가능 트래픽 개발계정 : 1,000 / 운영계정 : 활용사례 등록시 신청하면 트래픽 증가 가능
    model2 = datamodels.CHECKLIST_RealEstate()
    target = model2.target_ReqPool(dataName)

    pretty_title(f'{data_type} 수집대상')
    print(pd.DataFrame(target))
    # return 

    def __collect__(dataName, locationCode, tradeMonth):
        d = req_func(locationCode, tradeMonth)

        if d['resultCode'] == '00': 
            data = d.pop('data')
            """수집결과 업데이트"""
            model2.update_result(dataName, locationCode, tradeMonth, d)
            """데이타 저장"""
            model1.save_data(data, tradeMonth)
            return True 
        else: 
            logger.warning(d)
            return False


    _len = len(target)
    for i, (locationCode, tradeMonth) in enumerate(target, start=1):
        result = __collect__(dataName, locationCode, tradeMonth)
        # break
        if result: 
            print(data_type, f"수집중...{i}/{_len}") 
        else: 
            break
        sleep(0.5)

    logger.info([data_type, '수집완료'])





############################################################
"""DATA Analysis"""
############################################################


class AptTrade(object):

    def __init__(self): pass 
    def __load__(self, f=None, p=None): 
        model = _get_datamodel('아파트.*매매.*실거래')
        cursor = model.find(f, p)
        data = list(cursor)
        print({'DataLen': len(data)})
        df = pd.DataFrame(data)
        return df

    """아파트매매 전체데이타"""
    def analysis01(self):
        p = {'계약연월', '_id', '지역코드'}
        df = self.__load__(p=p)
        g1 = df.groupby('계약연월').count()
        g2 = df.groupby('지역코드').count()
        return g1, g2
    """지역코드별 데이타"""
    def analysis02(self):
        location = '강남구'
        o = datamodels.LocationCode().select({'법정동명': {'$regex': location}}, type='dcls')
        f = {
            '지역코드': o.지역코드,
        }
        p = {'계약연월', '_id'}
        df = self.__load__(f, p)
        return df.groupby('계약연월').count()


