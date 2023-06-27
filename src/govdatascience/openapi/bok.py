# -*- coding: utf-8 -*-
import json 
import os 


import requests
import pandas as pd


from ipylib.idebug import *


from govdatascience.openapi import APIKey


"""
정보-100 : 인증키가 유효하지 않습니다. 인증키를 확인하십시오! 인증키가 없는 경우 인증키를 신청하십시오!

정보-200 : 해당하는 데이터가 없습니다.

에러-100 : 필수 값이 누락되어 있습니다. 필수 값을 확인하십시오! 필수 값이 누락되어 있으면 오류를 발생합니다. 요청 변수를 참고 하십시오!

에러-101 : 주기와 다른 형식의 날짜 형식입니다.

에러-200 : 파일타입 값이 누락 혹은 유효하지 않습니다. 파일타입 값을 확인하십시오! 파일타입 값이 누락 혹은 유효하지 않으면 오류를 발생합니다. 요청 변수를 참고 하십시오!

에러-300 : 조회건수 값이 누락되어 있습니다. 조회시작건수/조회종료건수 값을 확인하십시오! 조회시작건수/조회종료건수 값이 누락되어 있으면 오류를 발생합니다.

에러-301 : 조회건수 값의 타입이 유효하지 않습니다. 조회건수 값을 확인하십시오! 조회건수 값의 타입이 유효하지 않으면 오류를 발생합니다. 정수를 입력하세요.

에러-400 : 검색범위가 적정범위를 초과하여 60초 TIMEOUT이 발생하였습니다. 요청조건 조정하여 다시 요청하시기 바랍니다.

에러-500 : 서버 오류입니다. OpenAPI 호출시 서버에서 오류가 발생하였습니다. 해당 서비스를 찾을 수 없습니다.

에러-600 : DB Connection 오류입니다. OpenAPI 호출시 서버에서 DB접속 오류가 발생했습니다.

에러-601 : SQL 오류입니다. OpenAPI 호출시 서버에서 SQL 오류가 발생했습니다.

에러-602 : 과도한 OpenAPI호출로 이용이 제한되었습니다. 잠시후 이용해주시기 바랍니다.
"""


def AUTH_KEY():
    try:
        return APIKey['BOK']
    except Exception as e:
        logger.error(e)



@ftracer 
def __req__(svcName, **kwargs):
    params = {
        '서비스명': svcName,
        '인증키': AUTH_KEY(),
        '요청유형': 'json',
        '언어구분': 'kr',
        '요청시작건수': '1',
        '요청종료건수': '1000',
    }
    params.update(kwargs)
    params = params.values()
    params = [p for p in params if p is not None]
    params = "/".join(params)
    url = "/".join(['https://ecos.bok.or.kr/api', params])
    print({'url': url})
    response = requests.get(url)
    print(response)
    try:
        d = json.loads(response.text)
    except Exception as e:
        print(response.text)
    else:
        return d[svcName]


@ftracer 
def getStatisticTableList(code=None):
    return __req__('StatisticTableList')


@ftracer 
def getStatisticWord(word='소비자동향지수'):
    return __req__('StatisticWord', 용어=word)
    

@ftracer 
def getStatisticItemList(code='601Y002'):
    return __req__('StatisticItemList', 통계표코드=code)
    

@ftracer 
def getStatisticSearch(code='200Y001', sYear='2000', eYear='2022'):
    return __req__('StatisticSearch', 
                   주기='A', 
                   검색시작일자=sYear, 
                   검색종료일자=eYear)
    

@ftracer 
def getKeyStatisticList():
    return __req__('KeyStatisticList')


@ftracer 
def getStatisticMeta(dataName='경제심리지수'):
    return __req__('StatisticMeta', 데이터명=dataName)

