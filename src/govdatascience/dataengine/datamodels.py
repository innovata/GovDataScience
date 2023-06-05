# -*- coding: utf-8 -*-
import os 
from datetime import datetime


import pandas as pd


from ipylib.idebug import *


from govdatascience.dataengine import database, schmodels



"""법정동코드목록"""
class LocationCode(database.Collection):

    def __init__(self): super().__init__(self.__class__.__name__)
    def __create__(self):
        file = os.path.join('C:\pypjts', 'GovDataScience', 'Data', '법정동코드.txt')
        with open(file, 'r', encoding='utf-8') as f:
            data = []
            for i, line in enumerate(f.readlines()):
                elems = line.split('\t')
                elems = [e.strip() for e in elems]
                # print([i, line, elems])
                if i == 0: columns = elems 
                else: data.append(elems)

        if len(data) == 0:
            logger.error('데이타없음')
        else:
            df = pd.DataFrame(data, columns=columns)
            data = df.to_dict('records')
            for d in data:
                d['지역코드'] = d['법정동코드'][:5]
            
            self.drop()
            self.insert_many(data)
    def view(self):
        codes = self.distinct('지역코드', {'폐지여부': '존재'})
        print({'CodesLen': len(codes)})
        f = {
            '폐지여부': '존재',
            # '법정동명': {'$regex': '특별.+시$|광역시$'},
            # '법정동명': {'$regex': '서울.+구$'},
            # '법정동명': {'$regex': '경기.+시$'},
            # '법정동명': {'$regex': '인천.+구$'},
            # '법정동명': {'$regex': '광역시.+구$'},
            '법정동명': {'$regex': '서울.+구$|경기.+시$|인천.+구$|광역시.+구$'},
            # '법정동명': {'$regex': '구$|시$'},
            # '법정동명': {'$regex': '도.+시$'},
            # '지역코드': {'$regex': '[1-9]{4}0'},
            # '지역코드': {'$regex': '[1-9]{3}00'},
            # '지역코드': '41480',
        }
        data = self.load(f)
        df = pd.DataFrame(data)
        df = df.drop_duplicates(subset=['지역코드'], keep='first').\
                reset_index(drop=True)
        print({'FrameLen': len(df)})
        return df
    def target01(self, region):
        """수집할 지역 정의"""
        if region == '서울': pat = '서울.+구$'
        elif region == '경기': pat = '경기.+시$'
        elif region == '인천': pat = '인천.+구$'
        elif region == '광역': pat = '광역시.+구$'

        f = {
            '폐지여부': '존재',
            '지역코드': {'$regex': '[1-9]{4}0'},
            '법정동명': {'$regex': pat},
        }
        p = {c:1 for c in ['법정동명', '지역코드']}
        p.update({'_id':0})
        data = self.load(f, p)
        return pd.DataFrame(data)




############################################################
"""체크리스트"""
############################################################

"""지역코드 기반 데이타수집 체크리스트 테이블 기본데이타 생성"""
def __CREATE_CHECKLIST_TYPE01__(model1):
    """수집할 지역 정의"""
    f = {
        '폐지여부': '존재',
        # '법정동명': {'$regex': '구$|시$'},
        '법정동명': {'$regex': '서울.+구$|경기.+시$|인천.+구$|광역시.+구$'},
        '지역코드': {'$regex': '[1-9]{4}0'},
    }
    model2 = LocationCode()
    p = {c:1 for c in ['법정동명', '지역코드']}
    p.update({'_id':0})
    data = model2.load(f, p)

    """수집할 계약연월 정의"""
    dts = pd.bdate_range(start='1990/1', end='2023/6', freq='M')
    dts = sorted(dts, reverse=True)
    tradeMonths = [t.strftime("%Y%m") for t in dts]
    
    model1.drop()
    for tradeMonth in tradeMonths:
        df = pd.DataFrame(data)
        df['계약연월'] = tradeMonth
        model1.insert_many(df.to_dict('records'))


"""국토교통부 부동산 관련 OpenAPI 데이타 수집이력"""
class CHECKLIST_RealEstate(database.Collection):

    def __init__(self): super().__init__(self.__class__.__name__)
    def __create__(self): __CREATE_CHECKLIST_TYPE01__(self)
    def upsert_doc(self, locationCode, tradeMonth, dataName, doc):
        f = {
            '지역코드': locationCode, 
            '계약연월': tradeMonth,
            'dataName': dataName
        }
        doc.update(f)
        self.update_one(f, {'$set': doc}, True)

    """OpenAPI데이타리스트"""
    def OpenAPIDataNames(self):
        return [
            'getRTMSDataSvcAptTradeDev',
            'getRTMSDataSvcAptRent',
            'getRTMSDataSvcRHTrade',
            'getRTMSDataSvcRHRent',
        ]

    def build_ReqPool(self, region):
        """수집할 지역 정의"""
        if region == '서울': pat = '서울.+구$'
        elif region == '경기': pat = '경기.+시$'
        elif region == '인천': pat = '인천.+구$'
        elif region == '광역': pat = '광역시.+구$'

        f = {
            '폐지여부': '존재',
            '지역코드': {'$regex': '[1-9]{4}0'},
            # '법정동명': {'$regex': '구$|시$'},
            # '법정동명': {'$regex': '서울.+구$|경기.+시$|인천.+구$|광역시.+구$'},
            '법정동명': {'$regex': pat},
        }
        model1 = LocationCode()
        p = {c:1 for c in ['법정동명', '지역코드']}
        p.update({'_id':0})
        data = model1.load(f, p)
        # return pd.DataFrame(data)

        """수집할 계약연월 정의"""
        def __tradeMonthsPool__():
            dts = pd.bdate_range(start='1990/1', end='2023/6', freq='M')
            dts = sorted(dts, reverse=True)
            tradeMonths = [t.strftime("%Y%m") for t in dts]
            return tradeMonths
        
        pool = __tradeMonthsPool__()

        def __target_tradeMonths__(locationCode, dataName):
            f = {
                '지역코드': locationCode,
                'dataName': dataName,
            }
            existed = self.distinct('계약연월', f)
            targets = [tradeMonth for tradeMonth in pool if tradeMonth not in existed]
            return targets

        dataNames = self.OpenAPIDataNames()

        """빌드"""
        newdata = []
        for d in data:
            locationCode = d['지역코드']
            for dataName in dataNames:
                tradeMonths = __target_tradeMonths__(locationCode, dataName)
                for tradeMonth in tradeMonths:
                    newdata.append({
                        '법정동명': d['법정동명'],
                        '지역코드': locationCode,
                        '계약연월': tradeMonth,
                        'dataName': dataName,
                    })
        df = pd.DataFrame(newdata)
        print({'FrameLen': len(df)})
        # return df
        
        if len(newdata) > 0:
            self.insert_many(newdata)
            logger.info(['데이타수집Pool생성완료', region])
        else: 
            logger.warning(['추가수집Pool없음', region])

    """OpenAPI신규수집대상"""
    def target_ReqPool(self, dataName, limit=1000):
        f = {
            '법정동명': {'$regex': '서울.+구$'},
            'dataName': dataName,
            'totalCount': None,
        }
        data = self.load(f, sort=[('계약연월',-1), ('법정동명',1)], limit=limit)
        return pd.DataFrame(data)

    def view(self):
        # self._manipulate01()

        f = {
            # '법정동명': {'$regex': '서울'},
            'dataName': {'$ne': None},
            # 'dataName': None,
            # 'totalCount': None
        }
        data = self.load(f, sort=[('계약연월',-1)])
        df = pd.DataFrame(data)
        print({'FrameLen': len(df)})
        return df 
    def _manipulate01(self):
        # self.update_many(
        #     {},
        #     {'$unset': {'계약연월': ''}}
        # )

        self.update_many(
            {},
            {'$rename': {'검색연월': '계약연월'}}
        )

        # self.delete_many(
        #     {'dataName': None}
        # )
    def _migrate01(self):
        self.drop()
        models = [
            CHECKLIST_AptTradeRealContract,
            CHECKLIST_AptRent,
            CHECKLIST_VillaTradeRealContract,
            CHECKLIST_VillaRent,
        ]
        for model in models:
            model = model()
            print(model)
            cursor = model.find({}, {'_id':0})
            data = list(cursor)
            self.insert_many(data)






"""아파트매매 실거래가 수집이력"""
class CHECKLIST_AptTradeRealContract(database.Collection):

    def __init__(self): super().__init__(self.__class__.__name__)
    def __create__(self): __CREATE_CHECKLIST_TYPE01__(self)
    def view(self):
        # self._manipulate01()

        f = {
            # '법정동명': {'$regex': '서울.+강남'},
            # 'resultCode': {'$ne': None},
        }
        data = self.load(f, sort=[('검색연월',-1)])
        df = pd.DataFrame(data)
        print({'FrameLen': len(df)})
        return df 
    def _manipulate01(self):
        self.update_many(
            {},
            # {'totalCount': {'$gt': 0}},
            # {'$set': {'dataName': 'getRTMSDataSvcAptTradeDev'}}
            {'$rename': {'개수': 'totalCount'}}
        )



"""국토교통부_아파트 전월세 수집이력"""
class CHECKLIST_AptRent(database.Collection):

    def __init__(self): super().__init__(self.__class__.__name__)
    def __create__(self): __CREATE_CHECKLIST_TYPE01__(self)
    def view(self):
        # self._manipulate01()

        f = {

        }
        data = self.load(f)
        df = pd.DataFrame(data)
        print({'FrameLen': len(df)})
        return df 
    def _manipulate01(self):
        self.update_many(
            {},
            {'$unset': {'data': ''}}
        )


"""국토교통부_연립다세대 매매 실거래자료 수집이력"""
class CHECKLIST_VillaTradeRealContract(database.Collection):

    def __init__(self): super().__init__(self.__class__.__name__)
    def __create__(self): __CREATE_CHECKLIST_TYPE01__(self)
    def view(self):
        # self._manipulate01()

        f = {

        }
        data = self.load(f)
        df = pd.DataFrame(data)
        print({'FrameLen': len(df)})
        return df     


"""국토교통부_연립다세대 전월세 자료 수집이력"""
class CHECKLIST_VillaRent(database.Collection):

    def __init__(self): super().__init__(self.__class__.__name__)
    def __create__(self): __CREATE_CHECKLIST_TYPE01__(self)
    def view(self):
        # self._manipulate01()

        f = {

        }
        data = self.load(f)
        df = pd.DataFrame(data)
        print({'FrameLen': len(df)})
        return df 
    



############################################################
"""체크리스트"""
############################################################

"""아파트매매 실거래 상세 자료"""
class AptTradeRealContract(database.Collection):

    def __init__(self):
        super().__init__(self.__class__.__name__)
    def inspect(self):
        sch = schmodels.RealState()
        database.print_colunqval(self, sch.columns)
    def load_frame(self, f, p, **kwargs):
        print({'필터': f})
        data = self.load(f, p, **kwargs)
        # print({'DataLen': len(data)})
        try:
            for d in data: d['계약일자'] = d['계약일자'].astimezone()
        except Exception as e: pass 
        return pd.DataFrame(data)
    def view(self):
        # self._manipulate01()

        o = LocationCode().select({'법정동명': {'$regex': '강남구'}})

        f = {
            # '지역코드': o.지역코드,
            '지역1': '서울특별시',
            '지역2': {'$regex': '노원'},
            # '계약연월': None,
            # '계약연월': {'$ne': None},
            # '아파트': {'$regex': '현대2차'},
            # '지역1': {'$regex': '^서울'}
            # '일련번호': '11560-75',
            '거래유형': '직거래',
        }
        sch = schmodels.RealState()
        cols = sch.get_cols(column='코드$|번호$|^해제')
        cols.remove('일련번호')
        
        p = {c:0 for c in cols}
        df = self.load_frame(f, p, sort=[('년',-1), ('월',-1), ('일',-1)])
        # df = df.drop_duplicates(subset=['지역코드'], keep='first').\
        #         reset_index(drop=True)
        print({'FrameLen': len(df)})

        def __groupby01__(df):
            g = df.groupby('아파트').count()
            print(g.sort_values('거래금액', ascending=False))

        __groupby01__(df)
        return df
    def clean_values_by_dtypes(self):
        cursor = self.find()
        data = list(cursor)
        sch = schmodels.ApartmentRealTrade()
        try:
            data = sch.parse_data(data)
        except Exception:
            pass 
        else:
            # df = pd.DataFrame(data)
            # return df 
            self.drop()
            self.insert_many(data)

    def assign_newColumns01(self):
        model = LocationCode()
        df = model.target01('서울')
        data = df.to_dict('records')
        for d in data:
            region1, region2 = d['법정동명'].split(' ')
            region1 = region1.strip()
            region2 = region2.strip()
            print([region1, region2])
            locationCode = d['지역코드']
            self.update_many(
                {'지역코드': locationCode},
                {'$set': {'지역1': region1, '지역2': region2}}
            )
    def assign_newColumns02(self):

        """계약일자 컬럼추가"""
        def __ContractDate__(d, _doc):
            dt = datetime(d['년'], d['월'], d['일']).astimezone()
            _doc.update({'계약일자': dt})
            return _doc
        """아파트ID 컬럼추가"""
        def __AptId__(d, _doc):
            _doc.update({'AptId': d['일련번호'] + '_' + str(d['전용면적'])})
            return _doc 
        
        cursor = self.find(limit=10)
        doc = {}
        for d in list(cursor):
            doc = __ContractDate__(d, doc)
            doc = __AptId__(d, doc)
            # print(doc)
            self.update_one(
                {'_id': d['_id']},
                {'$set': doc}
            )
    
    def _manipulate01(self):
        self.update_many(
            {},
            {'$rename': {'검색연월': '계약연월'}}
        )
    


"""국토교통부_아파트 전월세 자료"""
class AptRent(database.Collection):

    def __init__(self): super().__init__(self.__class__.__name__)
    def view(self):
        f = {
            # '계약연월': None,
            '아파트': '신답경남',
        }
        data = self.load(f)
        df = pd.DataFrame(data)
        # df = df.drop_duplicates(subset=['지역코드'], keep='first').\
        #         reset_index(drop=True)
        print({'FrameLen': len(df)})
        return df 



"""국토교통부_연립다세대 매매 실거래자료"""
class VillaTradeRealContract(database.Collection):

    def __init__(self): super().__init__(self.__class__.__name__)
    def view(self):
        f = {
            # '계약연월': None,
        }
        data = self.load(f)
        df = pd.DataFrame(data)
        print({'FrameLen': len(df)})
        return df 


"""국토교통부_연립다세대 전월세 자료"""
class VillaRent(database.Collection):

    def __init__(self): super().__init__(self.__class__.__name__)
    def view(self):
        f = {
            # '계약연월': None,
        }
        sch = schmodels.RealState()
        cols = sch.get_cols(column='코드$|번호$')
        pp.pprint(cols)
        p = {c:0 for c in cols}
        data = self.load(f, p)
        df = pd.DataFrame(data)
        print({'FrameLen': len(df)})
        return df 





"""한국은행 OpenAPI 서비스 통계 목록"""
class BOKStatisticTableList(database.Collection):

    def __init__(self): super().__init__(self.__class__.__name__)
    def view(self):
        f = {
            # 'STAT_CODE': {'$regex': '[A-Z0-9]{7}'},
            'CYCLE': {'$ne': None},
        }
        p = {c:0 for c in []}
        p.update({'_id': 0})
        data = self.load(f, p)
        df = pd.DataFrame(data)
        return df 