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




"""통계청 OpenAPI 인증 액세스토큰"""
def get_accessToken():
    url = 'https://sgisapi.kostat.go.kr/OpenAPI3/auth/authentication.json'
    params = {
        'consumer_key': APIKey.SGIS.ConsumerKey,
        'consumer_secret': APIKey.SGIS.ConsumerSecret,
    }
    response = requests.get(url, params=params)
    # print(response)
    # pp.pprint(response.__dict__)
    # print(response.text)
    d = json.loads(response.text)
    # pp.pprint(d)
    return d['result']['accessToken']



"""통계청 총조사 주요지표::총조사 주요지표 조회 API"""
@ftracer 
def getPopulation(year='2021', adm_cd=None, low_search='0'):
    url = 'https://sgisapi.kostat.go.kr/OpenAPI3/stats/population.json'
    params = {
        'accessToken': get_accessToken(),
        'year': year,
        'adm_cd': adm_cd,
        'low_search': low_search,
    }
    response = requests.get(url, params=params)
    print(response)
    # pp.pprint(response.__dict__)
    try:
        d = json.loads(response.text)
    except Exception as e:
        print(response.text)
    else:
        # pp.pprint(d)
        return d


"""통계청 인구통계::(인구주택총조사) 인구 통계 제공 API"""
@ftracer 
def getSearchPopulation(year='2021', gender=None, adm_cd=None, low_search=None, age_type=None, edu_level=None, mrg_state=None):
    url = 'https://sgisapi.kostat.go.kr/OpenAPI3/stats/searchpopulation.json'
    params = {
        'accessToken': get_accessToken(),
        'year': year,
        'gender': gender,
        'adm_cd': adm_cd,
        'low_search': low_search,
        'age_type': age_type,
        'edu_level': edu_level,
        'mrg_state': mrg_state,
    }
    response = requests.get(url, params=params)
    print(response)
    # pp.pprint(response.__dict__)
    # print(response.text)
    d = json.loads(response.text)
    # pp.pprint(d)
    return d


"""통계청 가구통계::(인구주택총조사) 가구 통계 제공 API"""
@ftracer 
def getHouseHold(year='2021', gender=None, adm_cd=None, low_search=None, household_type=None, ocptn_type=None):
    url = 'https://sgisapi.kostat.go.kr/OpenAPI3/stats/household.json'
    params = {
        'accessToken': get_accessToken(),
        'year': year,
        'gender': gender,
        'adm_cd': adm_cd,
        'low_search': low_search,
        'household_type': household_type,
        'ocptn_type': ocptn_type,
    }
    response = requests.get(url, params=params)
    print(response)
    d = json.loads(response.text)
    return d



"""통계청 주택통계::(인구주택총조사) 주택 통계 제공 API"""
@ftracer 
def getHouse(year='2021', adm_cd=None, low_search=None, house_type=None, const_year=None, house_area_cd=None, house_use_prid_cd=None):
    url = 'https://sgisapi.kostat.go.kr/OpenAPI3/stats/house.json'
    params = {
        'accessToken': get_accessToken(),
        'year': year,
        'adm_cd': adm_cd,
        'low_search': low_search,
        'house_type': house_type,
        'const_year': const_year,
        'house_area_cd': house_area_cd,
        'house_use_prid_cd': house_use_prid_cd,
    }
    response = requests.get(url, params=params)
    print(response)
    try:
        d = json.loads(response.text)
    except Exception as e:
        print(response.text)
    else:
        return d
    

"""통계청 사업체통계::(전국사업체조사) 사업체 통계 조회 API"""
@ftracer 
def getCompany(year='2021', adm_cd=None, low_search=None, class_code=None, theme_cd=None):
    url = 'https://sgisapi.kostat.go.kr/OpenAPI3/stats/company.json'
    params = {
        'accessToken': get_accessToken(),
        'year': year,
        'adm_cd': adm_cd,
        'low_search': low_search,
        'class_code': class_code,
        'theme_cd': theme_cd,
    }
    response = requests.get(url, params=params)
    print(response)
    try:
        d = json.loads(response.text)
    except Exception as e:
        print(response.text)
    else:
        return d
    

"""통계청 가구원통계::(농림어업총조사) 농가ㆍ임가ㆍ어가 가구원 통계 제공 API"""
@ftracer 
def getHouseHoldMember(year='2021', data_type=None, adm_cd=None, low_search=None, gender=None, age_from=None, age_to=None):
    url = 'https://sgisapi.kostat.go.kr/OpenAPI3/stats/householdmember.json'
    params = {
        'accessToken': get_accessToken(),
        'year': year,
        'data_type': data_type,
        'adm_cd': adm_cd,
        'low_search': low_search,
        'gender': gender,
        'age_from': age_from,
        'age_to': age_to,
    }
    response = requests.get(url, params=params)
    print(response)
    try:
        d = json.loads(response.text)
    except Exception as e:
        print(response.text)
    else:
        return d


