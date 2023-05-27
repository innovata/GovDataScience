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
def collectAptRealTrades():
    f = {'$or': [
        {'개수': None},
        {'개수': 0},
    ]}
    model1 = datamodels.AptRealTradesCollectChecklist()
    data = model1.load(f)
    df = pd.DataFrame(data)
    locationCodes = list(df['지역코드'])
    tradeMonths = list(df['연월'])

    def __collect__(locationCode, tradeMonth):
        response = openapi.getRTMSDataSvcAptTradeDev(locationCode, tradeMonth)
        print(response.text)
        root = ET.fromstring(response.text)
        cnt = root.find('body/totalCount').text
        print({'cnt': cnt})
        if int(cnt) == 0: 
            logger.warning('itemsLen is 0')
        else: 
            items = root.find('body/items')
            ElementTree(items).write('output.xml', encoding='UTF-8')
            df = pd.read_xml('output.xml')
            print(df)
            if len(df) > 0:
                df['연월'] = tradeMonth
                model2 = datamodels.ApartmentRealTrade()
                model2.insert_many(df.to_dict('records'))

    i = 0
    for tradeMonth, locationCode in itertools.product(tradeMonths, locationCodes):
        print([locationCode, tradeMonth])
        i+=1
        # __collect__(locationCode, tradeMonth)
    print(i)


    
