# -*- coding: utf-8 -*-
import itertools
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ElementTree
import re 
from time import sleep


import pandas as pd


from ipylib.idebug import *


from govdatascience import openapi
from govdatascience.dataengine import datamodels, schmodels



"""국토교통부_아파트매매 실거래 상세 자료"""
@ftracer
def collectAptTradeRealContracts():
    f = {'$or': [
        {'개수': None},
        {'개수': 0},
    ]}
    p = {'_id': 0}
    model1 = datamodels.CHECKLIST_AptTradeRealContract()
    # 신청가능 트래픽 개발계정 : 1,000 / 운영계정 : 활용사례 등록시 신청하면 트래픽 증가 가능
    data = model1.load(f, p, sort=[('계약연월',-1), ('지역코드',1)], limit=1000)
    df = pd.DataFrame(data)
    print(df)

    def __collect__(locationCode, tradeMonth):
        d = openapi.getRTMSDataSvcAptTradeDev(locationCode, tradeMonth)
        if d is None:
            return False 
        else:
            """다운로드 개수 업데이트"""
            f = {'지역코드': locationCode, '계약연월': tradeMonth}
            data = d.pop('data')
            d.update(f)
            model1.update_one(f, {'$set': d})                    
            
            """DB저장"""
            df = pd.DataFrame(data)
            if len(df) > 0:
                df['계약연월'] = tradeMonth
                _data = df.to_dict('records')

                sch = schmodels.RealState()
                _data = sch.parse_data(_data)
                
                model2 = datamodels.AptTradeRealContract()
                model2.insert_many(_data)
            return True

    _len = len(data)
    for i, d in enumerate(data, start=1):
        result = __collect__(d['지역코드'], d['계약연월'])
        # break
        if result: 
            print('-'*100, f"{i}/{_len}", '완료') 
        else: 
            break
    logger.info('아파트매매 실거래가 수집완료')



"""국토교통부_아파트 전월세 자료"""
@ftracer
def collectAptRents():
    f = {'$or': [
        {'numOfRows': None},
        {'numOfRows': 0},
    ]}
    p = {'_id': 0}
    model1 = datamodels.CHECKLIST_AptRent()
    # 신청가능 트래픽 개발계정 : 1,000 / 운영계정 : 활용사례 등록시 신청하면 트래픽 증가 가능
    data = model1.load(f, p, sort=[('계약연월',-1), ('지역코드',1)], limit=1000)
    df = pd.DataFrame(data)
    pretty_title('아파트 전월세 수집대상')
    print(df)

    def __collect__(locationCode, tradeMonth):
        d = openapi.getRTMSDataSvcAptRent(locationCode, tradeMonth)
        if d is None:
            return False 
        else:
            """다운로드 개수 업데이트"""
            f = {'지역코드': locationCode, '계약연월': tradeMonth}
            data = d.pop('data')
            d.update(f)
            model1.update_one(f, {'$set': d})
            
            """DB저장"""
            df = pd.DataFrame(data)
            if len(df) > 0:
                df['계약연월'] = tradeMonth
                _data = df.to_dict('records')
                
                sch = schmodels.RealState()
                _data = sch.parse_data(_data)

                model2 = datamodels.AptRent()
                model2.insert_many(df.to_dict('records'))
            return True

    _len = len(data)
    for i, d in enumerate(data, start=1):
        result = __collect__(d['지역코드'], d['계약연월'])
        # break
        if result: 
            print('-'*100, f"{i}/{_len}", '완료') 
        else: 
            break

    logger.info('아파트 전월세 수집완료')




"""국토교통부_부동산거래자료 통합수집기"""
@ftracer
def collect_RealState(data_type):
    sch = schmodels.RealState()

    if re.search('아파트.*매매.*실거래',  data_type) is not None:
        model2 = datamodels.AptTradeRealContract()
        dataName = 'getRTMSDataSvcAptTradeDev'
    elif re.search('아파트.*전월세',  data_type) is not None:
        model2 = datamodels.AptRent()
        dataName = 'getRTMSDataSvcAptRent'
    elif re.search('연립.*매매.*실거래',  data_type) is not None:
        model2 = datamodels.VillaTradeRealContract()
        dataName = 'getRTMSDataSvcRHTrade'
    elif re.search('연립.*전월세',  data_type) is not None:
        model2 = datamodels.VillaRent()
        dataName = 'getRTMSDataSvcRHRent'
    elif re.search('아파트.*분양권.*전매',  data_type) is not None:
        model2 = datamodels.VillaRent()
        dataName = 'getRTMSDataSvcSilvTrade'
    else:
        raise 


    # 신청가능 트래픽 개발계정 : 1,000 / 운영계정 : 활용사례 등록시 신청하면 트래픽 증가 가능
    model1 = datamodels.CHECKLIST_RealEstate()
    df = model1.target_ReqPool(dataName, limit=1000)
    req_func = getattr(openapi, dataName)

    pretty_title(f'{data_type} 수집대상')
    print(df)
    # return df 
    data = df.to_dict('records')

    def __collect__(locationCode, tradeMonth):
        d = req_func(locationCode, tradeMonth)
        if d is None:
            return False 
        else:
            """다운로드 개수 업데이트"""
            f = {'지역코드': locationCode, '계약연월': tradeMonth}
            data = d.pop('data')
            d.update(f)
            model1.update_one(f, {'$set': d})
            
            """DB저장"""
            df = pd.DataFrame(data)
            if len(df) > 0:
                df['계약연월'] = tradeMonth
                _data = df.to_dict('records')
                
                _data = sch.parse_data(_data)

                model2.insert_many(df.to_dict('records'))
            return True

    _len = len(data)
    for i, d in enumerate(data, start=1):
        result = __collect__(d['지역코드'], d['계약연월'])
        # break
        if result: 
            print(data_type, f"수집중...{i}/{_len}") 
        else: 
            break
        sleep(0.5)

    logger.info(['수집완료', data_type])

    