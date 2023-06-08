# -*- coding: utf-8 -*-
import itertools
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ElementTree
import re 
from time import sleep
from datetime import datetime, timedelta


import pandas as pd


from ipylib.idebug import *


from govdatascience.openapi import datagokr
from govdatascience.dataengine import datamodels, schmodels





"""수집할 계약연월 정의"""
def __tradeMonthPool__():
    end = datetime.now() + timedelta(days=31)
    end = end.strftime('%Y/%m')
    dts = pd.bdate_range(start='1990/1', end=end, freq='M')
    dts = sorted(dts, reverse=True)
    tradeMonths = [t.strftime("%Y%m") for t in dts]
    return tradeMonths


"""수집할 지역코드 정의"""
def __locationCodePool__(dataName):
    f = {
        '법정동명': {'$regex': '서울.*강남구$'},
        'dataName': dataName,
        # 'totalCount': None,
        # 'totalCount': {'$gt': 0},
    }
    model = datamodels.LocationCode()
    model
    return 




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
    req_func = getattr(datagokr, dataName)

    pretty_title(f'{data_type} 수집대상')
    print(df)
    # return df 
    target = df.to_dict('records')

    def __collect__(locationCode, tradeMonth):
        d = req_func(locationCode, tradeMonth)
        data = d.pop('data')

        """수집결과 업데이트"""
        model1.update_result(d, locationCode, tradeMonth)
        
        """데이타 저장"""
        model2.save_data(data, tradeMonth)
        
        return True if d['resultCode'] == '00' else False

    _len = len(target)
    for i, d in enumerate(target, start=1):
        result = __collect__(d['지역코드'], d['계약연월'])
        # break
        if result: 
            print(data_type, f"수집중...{i}/{_len}") 
        else: 
            break
        sleep(0.5)

    logger.info(['수집완료', data_type])

    