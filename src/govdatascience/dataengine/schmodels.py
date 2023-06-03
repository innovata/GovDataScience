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




class FieldType:

    def __init__(self, **kwargs):
        for k,v in kwargs.items(): setattr(self, k, v)



"""일자, 일시, 시간 데이터타입 컬럼"""
class DatetimeType(FieldType):

    def __init__(self, **kwargs): super().__init__(**kwargs)
    def parse(self, value):
        v = idatetime.DatetimeParser(value)
        if isinstance(v, datetime.datetime): return v
        else: return value


class PercentType(FieldType):

    def __init__(self, prec=4, **kwargs): super().__init__(prec=prec, **kwargs)
    def parse(self, value): return inumber.Percent(value, self.prec).value


class IntegerType(FieldType):

    def __init__(self, **kwargs): super().__init__(**kwargs)
    def parse(self, value): return inumber.iNumber(value).value


class StringType(FieldType):

    def __init__(self, **kwargs): super().__init__(**kwargs)
    def parse(self, value): return str(value)


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
    def view(self, f=None, p={'_id':0}, sort=[('dtype',1), ('column',1)], **kw):
        df = self.__view__(f, p, sort, **kw)
        return df.fillna('_')
    def __view__(self, f, p, sort, **kw):
        cursor = self.find(f, p, sort=sort, **kw)
        df = pd.DataFrame(list(cursor))
        return df.reindex(columns=self.SchemaStructure)
    def view01(self):
        self.delete_many({'column': None})
        return self.view()



class RealState(SchemaModel):

    def __init__(self): 
        # self.거래금액 = IntegerType()
        # self.건축년도 = IntegerType()
        # self.거래유형 = StringType()
        # self._columns = list(self.__dict__)
        # print(self.__dict__)

        super().__init__()
    def __create__(self):
        # cols = ['거래금액', '거래유형', '건축년도', '년', '도로명', '도로명건물본번호코드', '도로명건물부번호코드', '도로명시군구코드', '도로명일련번호코드', '도로명지상지하코드', '도로명코드', '법정동', '법정동본번코드', '법정동부번코드', '법정동시군구코드', '법정동읍면동코드', '법정동지번코드', '아파트', '월', '일', '일련번호', '전용면적', '중개사소재지', '지번', '지역코드', '층', '해제사유발생일', '해제여부', '검색연월']
        # cols = list(set(cols))
        # data = []
        # for c in cols:
        #     data.append((c, 'str', None, None))
        
        # self.drop()
        # for col, dtype, role, desc in data:
        #     self.insert_one({
        #         'column': col,
        #         'dtype': dtype,
        #         'role': role,
        #         'desc': desc,
        #     })

        # self.update_many(
        #     {'column': {'$regex': '^[년월일층]$|년도$|거래금액'}},
        #     {'$set': {'dtype': 'int'}}
        # )

        # self.update_many(
        #     {'column': {'$regex': '발생일$'}},
        #     {'$set': {'dtype': 'date'}}
        # )

        # self.update_many(
        #     {'column': {'$regex': '검색연월|지역코드'}},
        #     {'$set': {'desc': 'OpenAPI함수 입력값'}}
        # )

        self.update_many(
            {'column': {'$regex': '^거래금액$'}},
            {'$set': {'desc': '단위: 만원', 'unit': pow(10,4)}}
        )


