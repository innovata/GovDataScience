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













"""국토교통부_부동산거래자료 통합수집기"""
@ftracer
def collect_all_RealState():
    data_types = [
        '아파트매매 실거래',
        '아파트 전월세 자료',
        '연립다세대 매매 실거래자료',
        '연립다세대 전월세'
    ]
    for data_type in data_types:
        collect_one_RealState(data_type)


@ftracer
def collect_one_RealState(data_type):
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
    req_func = getattr(datagokr, dataName)


    # 신청가능 트래픽 개발계정 : 1,000 / 운영계정 : 활용사례 등록시 신청하면 트래픽 증가 가능
    model1 = datamodels.CHECKLIST_RealEstate()
    target = model1.target_ReqPool(dataName)

    pretty_title(f'{data_type} 수집대상')
    print(pd.DataFrame(target))
    # return 

    def __collect__(dataName, locationCode, tradeMonth):
        d = req_func(locationCode, tradeMonth)
        if d is None:
            return False
        else:
            if d['resultCode'] == '00': 
                data = d.pop('data')
                """수집결과 업데이트"""
                model1.update_result(dataName, locationCode, tradeMonth, d)
                """데이타 저장"""
                model2.save_data(data, tradeMonth)
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

    