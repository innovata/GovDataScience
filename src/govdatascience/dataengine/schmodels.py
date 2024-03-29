# -*- coding: utf-8 -*-
import os
import sys
import re
from datetime import datetime
import itertools


import pandas as pd


from ipylib.idebug import *
from ipylib import iparser, idatetime, inumber


from govdatascience import configuration as CONFIG
from govdatascience.dataengine import database
from govdatascience.dataengine.database import db




class SchemaModel(database.Collection):
    # column: 컬럼명
    # dtype: 데이터 타입
    # role: 역할
    # desc: 설명
    # unit: 숫자단위
    SchemaStructure = ['seq','column','dtype','role','desc','unit']
    SchemaKeyField = 'column'
    modelType = 'SchemaModel'

    def __init__(self, modelName=None):
        if modelName is None: modelName = self.__class__.__name__
        else: pass
        self.modelName = modelName
        super().__init__(f"_Schema_{modelName}")

        # try:
        #     def get_cols(f={}): return self.distinct('column', f)
        #     self.schema = get_cols()
        #     self.keycols = get_cols({'role':{'$regex':'key'}})
        #     self.numcols = get_cols({'dtype':{'$regex':'int|int_abs|float|pct'}})
        #     self.intcols = get_cols({'dtype':{'$regex':'int|int_abs'}})
        #     self.flcols = get_cols({'dtype':{'$regex':'float'}})
        #     self.pctcols = get_cols({'dtype':{'$regex':'pct'}})
        #     self.dtcols = get_cols({'dtype':{'$regex':'time|date|datetime'}})
        #     self.strcols = get_cols({'dtype':'str'})
        # except Exception as e:
        #     logger.error(e)

        # try:
        #     c = self.find({}, {'column':1}, sort=[('seq',1)])
        #     df = pd.DataFrame(list(c))
        #     self.colseq = list(df.column)
        # except Exception as e:
        #     self.colseq = None
    def __get_cols__(self, f={}): return self.distinct('column', f)
    def get_cols(self, **kw):
        f = {}
        for k,v in kw.items(): f.update({k: {'$regex': v}})
        return self.distinct('column', f)
    @property
    def columns(self): return self.__get_cols__()
    @property
    def allcols(self): return self.__get_cols__()
    @property
    def keycols(self): return self.__get_cols__({'role':{'$regex':'key'}})
    @property
    def numcols(self): return self.__get_cols__({'dtype':{'$regex':'int|int_abs|float|pct'}})
    @property
    def intcols(self): return self.__get_cols__({'dtype':{'$regex':'int|int_abs'}})
    @property
    def flcols(self): return self.__get_cols__({'dtype':{'$regex':'float'}})
    @property
    def pctcols(self): return self.__get_cols__({'dtype':{'$regex':'pct'}})
    @property
    def dtcols(self): return self.__get_cols__({'dtype':{'$regex':'time|date|datetime'}})
    @property
    def strcols(self): return self.__get_cols__({'dtype':'str'})
    @property
    def colseq(self):
        try:
            c = self.find({}, {'column':1}, sort=[('seq',1)])
            df = pd.DataFrame(list(c))
            return list(df.column)
        except Exception as e:
            pass

    def projection(self, cols, vis=1):
        p = {c: vis for c in cols}
        p.update({'_id':0})
        return p

    @property
    def DtypeDict(self):
        cursor = self.find(None, {'_id':0, 'column':1, 'dtype':1})
        return {d['column']:d['dtype'] for d in list(cursor)}
    @property
    def inputFormat(self):
        fmt = {}
        cursor = self.find(None, {'_id':0})
        for d in list(cursor):
            c = d['column']
            dtype = d['dtype']
            if dtype == 'bool': v = True
            elif dtype == 'str': v = None
            elif dtype == 'int': v = 0
            elif dtype == 'datetime': v = datetime.today().isoformat()[:10]
            elif dtype == 'list': v = []
            elif dtype == 'dict': v = {}
            else: raise
            fmt.update({c: v})
        return fmt

    """CSV파일 --> DB"""
    @ctracer
    def create(self, schemaName=None):
        schemaName = self.modelName if schemaName is None else schemaName
        file = os.path.join(CONFIG.SCHEMA_FILE_PATH, f'{schemaName}.csv')
        data = FileReader.read_csv(file)
        if data is None:
            logger.error({'data':data})
            raise
        else:
            # 컬럼 순서를 정해준다
            for i,d in enumerate(data): d['seq'] = i
            self.drop()
            self.insert_data(data)
    """DB --> CSV파일"""
    @ctracer
    def backup(self):
        cursor = self.find(None, {'_id':0})
        data = list(cursor)
        if len(data) == 0: pass
        else:
            file = os.path.join(CONFIG.SCHEMA_FILE_PATH, f'{self.modelName}.csv')
            print(file)
            pp.pprint(data)
            # FileWriter.write_csv(file, cols, data)
    def define_schemaStructure(self, li):
        if isinstance(li, list): self.SchemaStructure = li
        else: raise
    def add_schema(self, *args, **kwargs):
        if len(args) > 0:
            doc = {}
            columns = self.SchemaStructure.copy()
            columns.remove('seq')
            for k, v in zip(columns, args):
                if k == 'column': f = {k: v}
                doc.update({k: v})
            self.update_one(f, {'$set': doc}, True)
        elif len(kwargs) > 0:
            f = {'column': kwargs.get('column')}
            self.update_one(f, {'$set': kwargs}, True)
        else: raise

    def parse_value(self, field, value):
        # 'field'를 이용하여 dtype을 가져온다
        ddict = self.DtypeDict.copy()
        if field in ddict:
            dtype = ddict[field]
            return iparser.DtypeParser(value, dtype)
        else:
            return value
    def parse_data(self, data):
        if isinstance(data, dict): type, data = 'dict', [data]
        elif isinstance(data, list): type, data = 'list', data
        else: raise

        ddict = self.DtypeDict.copy()
        for d in data:
            for k,v in d.items():
                if k in ddict:
                    dtype = ddict[k]
                    if dtype in [None,'None']:
                        pass
                    else:
                        d[k] = iparser.DtypeParser(v, dtype)
        return data[0] if type == 'dict' else data
    def astimezone(self, data):
        dtcols = self.dtcols
        for d in data:
            for c in dtcols:
                if c in d:
                    v = idatetime.DatetimeParser(d[c])
                    if isinstance(v, datetime.datetime): d[c] = v
                    else: pass 
        return data
    def view(self, f=None, p={'_id':0}, sort=[('seq',1), ('dtype',1), ('column',1)], **kw):
        df = self.__view__(f, p, sort, **kw)
        return df.fillna('_')
    def __view__(self, f, p, sort, **kw):
        cursor = self.find(f, p, sort=sort, **kw)
        df = pd.DataFrame(list(cursor))
        return df.reindex(columns=self.SchemaStructure)
    def view01(self):
        self.delete_many({'column': None})
        return self.view()


class Field(object):

    def __init__(self, name, dtype, role=None, desc=None, **kwargs):
        self.name = name
        self.dtype = dtype
        self.role = role
        self.desc = desc
        for k, v in kwargs.items(): setattr(self, k, v)
    @property
    def dict(self):
        d = self.__dict__.copy()
        d['column'] = self.name
        del d['name']
        return d
    def parse(self, value): return iparser.DtypeParser(value, self.dtype)


class SchemaModelV2(SchemaModel):

    def __init__(self, modelName=None):
        super().__init__(modelName)
    def __build__(self, Fields):
        self._columns = []
        for field in Fields:
            setattr(self, field.name, field)
            self._columns.append(field.name)
    def save_to_db(self):
        self.drop()
        for i, col in enumerate(self._columns):
            field = getattr(self, col)
            d = field.dict
            d.update({'seq': i})
            pp.pprint(d)
            self.update_one(
                {'column': field.name},
                {'$set': d},
                True
            )
        logger.info(['DB저장완료', self])
    def save_to_csv(self):
        cursor = self.find({}, {'_id':0})
        df = pd.DataFrame(list(cursor))
        print(df.fillna('_'))
        os.makedirs(CONFIG.SCHEMA_FILE_PATH, exist_ok=True)
        filepath = os.path.join(CONFIG.SCHEMA_FILE_PATH, f'{self.modelName}.csv')
        df.to_csv(filepath, index=False)
        logger.info(['CSV파일저장완료', filepath])


class CHECKLIST_RealEstate(SchemaModelV2):

    def __init__(self): 
        super().__init__()
        self.build()
    def build(self):
        fields = [
            Field('dataName', 'str', desc='openapi.datgokr 함수명'),
            Field('계약연월', 'str', desc='YYYYMM 형태'),
            Field('지역코드', 'str', desc='LocationCode테이블 지역코드 컬럼'),
            Field('resultCode', 'str'),
            Field('resultMsg', 'str'),
            Field('numOfRows', 'int'),
            Field('pageNo', 'int'),
            Field('totalCount', 'int'),
        ]
        self.__build__(fields)


"""아파트매매/전세, 빌라매매/전세 데이타에 대한 스키마"""
class RealEstate(SchemaModelV2):

    def __init__(self): 
        super().__init__()
        self.build()
    def build(self):
        fields = [
            Field('거래금액', 'int', desc='단위:만원', unit=pow(10,4)),
            Field('보증금액', 'int'),
            Field('월세금액', 'int'),
            Field('종전계약보증금', 'int'),
            Field('종전계약월세', 'int'),
            Field('건축년도', 'int'),
            Field('년', 'int'),
            Field('월', 'int'),
            Field('일', 'int'),
            Field('층', 'int'),
            Field('거래유형', 'str'),
            Field('계약연월', 'str', desc='OpenAPI함수 입력값'),
            Field('지역코드', 'str', desc='OpenAPI함수 입력값'),
            Field('일련번호', 'str'),
            Field('거래유형', 'str'),
            Field('도로명', 'str'),
            Field('도로명건물본번호코드', 'str'),
            Field('도로명건물부번호코드', 'str'),
            Field('도로명시군구코드', 'str'),
            Field('도로명일련번호코드', 'str'),
            Field('도로명지상지하코드', 'str'),
            Field('도로명코드', 'str'),
            Field('법정동', 'str'),
            Field('법정동본번코드', 'str'),
            Field('법정동부번코드', 'str'),
            Field('법정동시군구코드', 'str'),
            Field('법정동읍면동코드', 'str'),
            Field('법정동지번코드', 'str'),
            Field('아파트', 'str'),
            Field('전용면적', 'str'),
            Field('중개사소재지', 'str'),
            Field('지번', 'str'),
            Field('해제여부', 'str'),
            Field('갱신요구권사용', 'str'),
            Field('지역1', 'str'),
            Field('지역2', 'str'),
            Field('해제여부', 'str'),
            Field('해제사유발생일', 'date'),
            Field('계약일자', 'datetime'),
        ]
        self.__build__(fields)



