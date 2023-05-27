# -*- coding: utf-8 -*-
import itertools
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ElementTree


import pandas as pd


from ipylib.idebug import *


from govdatascience import openapi
from govdatascience.dataengine import datamodels



"""국토교통부_아파트매매 실거래 상세 자료"""
@ftracer
def collectAptTradeRealContracts():
    f = {'$or': [
        {'개수': None},
        {'개수': 0},
    ]}
    p = {'_id': 0}
    model1 = datamodels.AptRealTradesCollectChecklist()
    # 신청가능 트래픽 개발계정 : 1,000 / 운영계정 : 활용사례 등록시 신청하면 트래픽 증가 가능
    data = model1.load(f, p, sort=[('검색연월',-1), ('지역코드',1)], limit=1000)
    df = pd.DataFrame(data)
    print(df)

    def __collect__(locationCode, tradeMonth):
        d = openapi.getRTMSDataSvcAptTradeDev(locationCode, tradeMonth)
        if d is None:
            return False 
        else:
            """다운로드 개수 업데이트"""
            f = {'지역코드': locationCode, '검색연월': tradeMonth}
            data = d.pop('data')
            d.update(f)
            model1.update_one(f, {'$set': d})                    
            
            """DB저장"""
            df = pd.DataFrame(data)
            if len(df) > 0:
                df['검색연월'] = tradeMonth
                model2 = datamodels.ApartmentRealTrade()
                model2.insert_many(df.to_dict('records'))
            return True

    _len = len(data)
    for i, d in enumerate(data, start=1):
        result = __collect__(d['지역코드'], d['검색연월'])
        # break
        if result: 
            print('-'*100, f"{i}/{_len}", '완료') 
        else: 
            break



"""국토교통부_아파트 전월세 자료"""
@ftracer
def collectAptRents():
    f = {'$or': [
        {'numOfRows': None},
        {'numOfRows': 0},
    ]}
    p = {'_id': 0}
    model1 = datamodels.AptRentsChecklist()
    # 신청가능 트래픽 개발계정 : 1,000 / 운영계정 : 활용사례 등록시 신청하면 트래픽 증가 가능
    data = model1.load(f, p, sort=[('검색연월',-1), ('지역코드',1)], limit=1000)
    df = pd.DataFrame(data)
    pretty_title('아파트 전월세 수집대상')
    print(df)

    def __collect__(locationCode, tradeMonth):
        d = openapi.getRTMSDataSvcAptRent(locationCode, tradeMonth)
        if d is None:
            return False 
        else:
            """다운로드 개수 업데이트"""
            f = {'지역코드': locationCode, '검색연월': tradeMonth}
            data = d.pop('data')
            d.update(f)
            model1.update_one(f, {'$set': d})
            
            """DB저장"""
            df = pd.DataFrame(data)
            if len(df) > 0:
                df['검색연월'] = tradeMonth
                model2 = datamodels.ApartmentRent()
                model2.insert_many(df.to_dict('records'))
            return True

    _len = len(data)
    for i, d in enumerate(data, start=1):
        result = __collect__(d['지역코드'], d['검색연월'])
        # break
        if result: 
            print('-'*100, f"{i}/{_len}", '완료') 
        else: 
            break

    
