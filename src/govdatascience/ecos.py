# -*- coding: utf-8 -*-
# 한국은행 Open Data


from govdatascience.openapi import bok as api

from govdatascience.dataengine import datamodels






def build_StatisticTableList():
    d = api.getStatisticTableList()
    data = d['row']
    
    model = datamodels.BOKStatisticTableList()
    model.drop()
    model.insert_many(data)
