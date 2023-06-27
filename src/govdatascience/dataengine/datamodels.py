# -*- coding: utf-8 -*-
import os 
from datetime import datetime, timedelta
from time import sleep
import subprocess
import sys


import pandas as pd


from ipylib.idebug import *


from govdatascience import configuration as CONF
from govdatascience.dataengine import database, schmodels






############################################################
"""메타데이타"""
############################################################

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
            '법정동명': {'$regex': '서울.+구$'},
            # '법정동명': {'$regex': '경기.+시$'},
            # '법정동명': {'$regex': '인천.+구$'},
            # '법정동명': {'$regex': '광역시.+구$'},
            # '법정동명': {'$regex': '서울.+구$|경기.+시$|인천.+구$|광역시.+구$'},
            # '법정동명': {'$regex': '구$|시$'},
            # '법정동명': {'$regex': '도.+시$'},
            # '지역코드': {'$regex': '[1-9]{4}0'},
            # '지역코드': {'$regex': '[1-9]{3}00'},
            # '지역코드': '11140',
        }
        data = self.load(f, sort=[('법정동명',1)])
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
    def target_ReqPool(self, dataName):

        """강남구 데이타의 계약연월리스트가 기준"""
        def has_stnd_data():
            f = {
                'dataName': dataName,
                '지역코드': '11680',# 강남구
            }
            n = self.count_documents(filter=f)
            return False if n == 0 else True
        
        # return has_stnd_data()

        """수집할 계약연월 정의"""
        def __tradeMonthPool01__():
            end = datetime.now() + timedelta(days=31)
            end = end.strftime('%Y/%m')
            dts = pd.bdate_range(start='1990/1', end=end, freq='M')
            dts = sorted(dts, reverse=True)
            trade_months = [t.strftime("%Y%m") for t in dts]
            return trade_months
        
        # return __tradeMonthPool01__()
        
        """기준데이타(강남구)의 수집된 계약연월"""
        def __tradeMonthPool02__():
            f = {
                'dataName': dataName,
                '지역코드': '11680',
                'totalCount': {'$gt': 0},
            }
            return sorted(self.distinct('계약연월', f), reverse=True)
        
        # return __tradeMonthPool02__()

        # 기준데이타(강남구)가 있는지 확인하고 케이스별로 자동으로 결정해서
        def __tradeMonthPool__():
            if has_stnd_data(): 
                return __tradeMonthPool02__()
            else: 
                return __tradeMonthPool01__()
        
        # return __tradeMonthPool__()

        # 기수집된 계약연월은 제거
        def __targetTradeMonth__(locationCode, trade_month_pool):
            f = {
                'dataName': dataName,
                '지역코드': locationCode,
            }
            collected = self.distinct('계약연월', f)
            return [p for p in trade_month_pool if p not in collected]
    
        """수집할 지역코드 정의"""
        def __locationCodePool__():
            if has_stnd_data():
                f = {'법정동명': {
                    '$regex': '서울.+구$',
                    '$not': {'$regex': '강남구$'},
                }}
            else:
                f = {'법정동명': {'$regex': '강남구$'}}
            model = LocationCode()
            data = model.load(f, sort=[('법정동명',1)])
            df = pd.DataFrame(data)
            location_codes = list(df.지역코드)
            # print({'LocationLen': len(location_codes)})
            return location_codes
        
        # return __locationCodePool__()

        loc_code_pool = __locationCodePool__()
        trade_month_pool = __tradeMonthPool__()
        print({'LocationLen': len(loc_code_pool)})
        print({'TradeMonthLen': len(trade_month_pool)})

        pool = []
        for locationCode in loc_code_pool:
            trade_months = __targetTradeMonth__(locationCode, trade_month_pool)
            for tradeMonth in trade_months:
                pool.append((locationCode, tradeMonth))
        print({'PoolLen': len(pool)})
        return pool

    def inspect(self):
        cols = ['법정동명', '지역코드', 'dataName', 'totalCount', '계약연월', 'numOfRows', 'pageNo', 'resultCode', 'resultMsg']
        database.print_colunqval(self, cols)
    def view(self):
        # self._manipulate01()
        # self._manipulate02()
        # self._manipulate03()
        # self._parse_data()

        f = {
            # '법정동명': {'$regex': '서울'},
            # 'dataName': {'$ne': None},
            # 'dataName': None,
            # 'dataName': 'getRTMSDataSvcAptRent',
            # 'totalCount': None,
            # 'totalCount': {'$ne': None},
            'totalCount': {'$gt': 0},
            # 'totalCount': 0,
            # '계약연월': {'$regex': '^2000'},
        }
        print({'필터': f})
        data = self.load(f, sort=[('계약연월',-1), ('법정동명',1), ('dataName',1)])
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
    def _manipulate02(self):
        f = {
            'totalCount': {'$ne': None},
        }
        self.update_many(
            f,
            {'$set': {
                'resultMsg': 'NORMAL SERVICE.',
                'pageNo': 1,
                'resultCode': '00',
                'numOfRows': 1000,
            }}
        )
    """중복제거"""
    def _manipulate03(self):
        cols = ['계약연월', '지역코드', 'dataName']
        p = {c:1 for c in cols}
        cursor = self.find({}, {})
        data = list(cursor)
        df = pd.DataFrame(data)
        print({'중복제거전': len(df)})
        _df = df.drop_duplicates(subset=cols)
        print({'중복제거후': len(_df)})
        
        TF = df.duplicated(subset=cols, keep='first')
        _df = df[TF]
        print({'_dfLen': len(_df)})
        if len(_df) == 0:
            logger.info('중복데이타없음')
        else: 
            _df = _df.reset_index(drop=True)
            _df = _df.dropna(axis=1, how='all')
            # # _df = _df.query('totalCount > 0')
            print(_df)
            # ids = list(_df._id)
            # print(ids)
            # self.delete_many({'_id': {'$in': ids}})
        
    """수집결과 업데이트"""
    @ctracer
    def update_result(self, dataName, locationCode, tradeMonth, d):
        f = {'dataName': dataName, '지역코드': locationCode, '계약연월': tradeMonth}
        # 데이타파싱
        d.update(f)
        for col in ['totalCount', 'numOfRows', 'pageNo']:
            d[col] = int(d[col])
        self.update_one(f, {'$set': d}, True)
    """데이터파싱"""
    def _parse_data(self):
        f = {
            'totalCount': {'$ne': None},
        }
        cursor = self.find(f)
        data = list(cursor)
        for d in data:
            self.update_one(
                {'_id': d['_id']},
                {'$set': {
                    'totalCount': int(d['totalCount'])
                }}
            )




############################################################
"""공공데이타"""
############################################################

class RealEstateBase(database.Collection):

    def __init__(self): super().__init__(self.__class__.__name__)
    def load_frame(self, f, p={'_id':0}, **kwargs):
        print({'DocsLen': self.count_documents(filter={})})
        print({'필터': f})
        data = self.load(f, p, **kwargs)
        print({'DataLen': len(data)})
        try:
            for d in data: d['계약일자'] = d['계약일자'].astimezone()
        except Exception as e: pass 
        df = pd.DataFrame(data)
        try:
            df = df.sort_values('계약일자', ascending=False).reset_index(drop=True)
        except Exception as e: pass 
        return df 
    # @ctracer
    def save_data(self, data, tradeMonth):
        if len(data) == 0:
            logger.warning('데이타없음')
        else:
            df = pd.DataFrame(data)
            df['계약연월'] = tradeMonth
            print(df)
            _data = df.to_dict('records')
            
            sch = schmodels.RealState()
            _data = sch.parse_data(_data)

            self.insert_many(_data)
    @ctracer
    def clean_values_by_dtypes(self):
        cursor = self.find()
        data = list(cursor)
        sch = schmodels.RealState()
        try:
            data = sch.parse_data(data)
        except Exception:
            pass 
        else:
            # df = pd.DataFrame(data)
            # return df 
            self.drop()
            self.insert_many(data)
        logger.info('데이타청소완료')
    """기 수집된 데이터 키값들"""
    def already_collected(self, locationCode):
        f = {
            # '지역2': {'$regex': '강남구'}
            '지역코드': locationCode,
        }
        p = {c:1 for c in ['지역코드', '계약연월']}
        df = self.load_frame(f, p, sort=[('계약연월',-1)])
        df = df.drop_duplicates(keep='first').reset_index(drop=True)
        return df 
    """중복제거"""
    def dedup_data(self):
        f = {
            # '지역코드': '11680',
        }
        cursor = self.find(f)
        data = list(cursor)
        print({'DataLen': len(data)})
        df = pd.DataFrame(data).\
            sort_values(['년', '월', '일'], ascending=False).\
            reset_index(drop=True)
        
        cols = list(df.columns)
        cols.remove('_id')
        subset = cols
        # subset = ['지역코드', '계약연월', '일련번호', '년', '월', '일', '아파트', '전용면적', '층', '거래금액']
        print({'subset': subset})

        net = df.drop_duplicates(subset=subset, keep='first').\
                reset_index(drop=True)
        # print(net)
        print({'NetLen': len(net)})

        TF = df.duplicated(subset=subset, keep='first')
        dup = df[TF]
        # print(dup)
        print({'DupLen': len(dup)})
    
        ids = list(dup._id)
        self.delete_many({'_id': {'$in': ids}})
    
        logger.info('중복제거완료')
        return dup, net
    def update_model_data(self):
        # assign_newColumns01(self)
        # assign_newColumns02(self)
        print('Update model')
        # for i in range(3):
        #     logger.info(i)
        #     sleep(1)
    def inspect(self):
        sch = schmodels.RealState()
        cols = sch.columns + ['계약연월','계약일자','지역1','지역2']
        database.print_colunqval(self, cols)


"""아파트매매 실거래 상세 자료"""
class AptTradeRealContract(RealEstateBase):

    def __init__(self): super().__init__()
    def view(self):
        # _manipulate01(self)
        # assign_newColumns01(self)
        # assign_newColumns02(self)

        o = LocationCode().select({'법정동명': {'$regex': '강남구'}}, type='dcls')
        f = {
            '지역코드': o.지역코드,
            # '지역1': '서울특별시',
            # '지역2': {'$regex': '강남구'},
            # '계약연월': None,
            # '계약연월': {'$ne': None},
            # '아파트': {'$regex': '현대2차'},
            # '지역1': {'$regex': '^서울'}
            # '일련번호': '11560-75',
            # '거래유형': '직거래',
        }
        sch = schmodels.RealState()
        cols = sch.get_cols(column='코드$|번호$|^해제')
        cols.remove('일련번호')
        
        p = {c:0 for c in cols}
        df = self.load_frame(f, p)

        # 중복제거
        def __dedup__(df):
            try:
                subset = ['지역코드']
                return df.drop_duplicates(subset=subset, keep='first').\
                        reset_index(drop=True)
            except Exception as e:
                logger.error(e)
                return df 
        
        df = __dedup__(df)
        print({'FrameLen': len(df)})
        
        # 정렬
        def __sort__(df):
            try:
                return df.sort_values('계약일자').reset_index(drop=True)
            except Exception as e:
                logger.error(e)
                return df

        def __groupby01__(df):
            try:
                g = df.groupby('아파트').count()
                print(g.sort_values('거래금액', ascending=False))
            except Exception as e:
                logger.error(e)
                return df 

        def __uniqueValues__(df):
            try:
                for c in list(df.columns):
                    values = list(df[c].unique())
                    print(c, len(values), values)
            except Exception as e:
                logger.error(e)

        # __groupby01__(df)
        __uniqueValues__(df)
        
        return df
    
    
"""국토교통부_아파트 전월세 자료"""
class AptRent(RealEstateBase):

    def __init__(self): super().__init__()
    def view(self):
        # assign_newColumns01(self)
        # assign_newColumns02(self)
        # _manipulate01(self)
        # self.clean_values_by_dtypes()

        # print({'DocsLen': self.count_documents(filter={})})
        # return 

        f = {
            # '계약연월': None,
            # '아파트': '신답경남',
            # '지역코드': '11140',
            # '지역1': {'$regex': '서울'},
            '지역2': {'$regex': '강남'},
            # '계약연월': None,
            # '계약연월': {'$ne': None},
            # '아파트': {'$regex': '현대2차'},
            # '지역1': {'$regex': '^서울'}
            # '일련번호': '11560-75',
            # '거래유형': '직거래',
        }
        df = self.load_frame(f)
        # df = df.drop_duplicates(subset=['지역코드'], keep='first').\
        #         reset_index(drop=True)
        print({'FrameLen': len(df)})
        df.info()
        print(list(df.지역2.unique()))
        return df 



"""국토교통부_연립다세대 매매 실거래자료"""
class VillaTradeRealContract(RealEstateBase):

    def __init__(self): super().__init__()
    def view(self):
        f = {
            # '계약연월': None,
        }
        data = self.load(f)
        df = pd.DataFrame(data)
        print({'FrameLen': len(df)})
        return df 


"""국토교통부_연립다세대 전월세 자료"""
class VillaRent(RealEstateBase):

    def __init__(self): super().__init__()
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




############################################################
"""한국은행 데이타"""
############################################################

"""한국은행 OpenAPI 서비스 통계 목록"""
class BOKStatisticTableList(database.Collection):

    def __init__(self): super().__init__(self.__class__.__name__)
    def view(self):
        f = {
            # 'STAT_CODE': {'$regex': '\d{3}Y\d{3}'},
            # 'CYCLE': {'$ne': None},
            'SRCH_YN': 'Y',
            'STAT_NAME': {'$regex': '국민'},
        }
        p = {c:0 for c in []}
        p.update({'_id': 0})
        data = self.load(f, p)
        df = pd.DataFrame(data)
        return df 
    




MODEL_LIST = vars()




############################################################
"""데이타 업데이터"""
############################################################


"""법정동명을 지역1/지역2로 나누기"""
def assign_newColumns01(model):
    f = {
        '폐지여부': '존재',
        '지역코드': {'$regex': '[1-9]{4}0'},
    }
    cursor = LocationCode().find(f)
    data = list(cursor)
    df = pd.DataFrame(data)
    df = df.drop_duplicates(subset=['지역코드'], keep='first')
    print(df.reset_index(drop=True))
    data = df.to_dict('records')
    # return 
    for d in data:
        try:
            region1, region2 = d['법정동명'].split(' ')
            region1 = region1.strip()
            region2 = region2.strip()
        except Exception:
            print(d)
            region1, region2 = d['법정동명'], None
        finally:
            locationCode = d['지역코드']
            logger.info(['업데이트중...', locationCode, region1, region2])
            model.update_many(
                {'지역코드': locationCode},
                {'$set': {'지역1': region1, '지역2': region2}}
            )

    logger.info('신규컬럼생성01완료')


def assign_newColumns02(model):

    """계약일자 컬럼추가"""
    def __ContractDate__(d, _doc):
        dt = datetime(d['년'], d['월'], d['일']).astimezone()
        _doc.update({'계약일자': dt})
        return _doc
    """아파트ID 컬럼추가"""
    def __AptId__(d, _doc):
        _doc.update({'AptId': d['일련번호'] + '_' + str(d['전용면적'])})
        return _doc 
    
    f = {'$or': [
            {'계약일자': None},
            {'AptId': None}
    ]}
    cursor = model.find(f)
    doc = {}
    for d in list(cursor):
        doc = __ContractDate__(d, doc)
        doc = __AptId__(d, doc)
        # print(doc)
        model.update_one(
            {'_id': d['_id']},
            {'$set': doc}
        )
    logger.info('신규컬럼생성02완료')


def _manipulate01(model):
    model.update_many(
        {},
        {'$rename': {'검색연월': '계약연월'}}
    )



def update_model_data(model):
    assign_newColumns01(model)
    assign_newColumns02(model)
    logger.info(['모델데이타업데이트종료', model])


def multiprocessing_updateModelData():
    filepath = os.path.join(CONF.PROJECT_PATH, 'src\govdatascience\dataengine\_exec_files\datamodels01.py')
    filepath = os.path.realpath(filepath)

    argv = sys.argv[1:]
    argv.append('update_model_data')
    models = 'AptTradeRealContract|AptRent|VillaTradeRealContract|VillaRent'
    argv.append(models)

    rv = subprocess.run([sys.executable, filepath] + argv)
    logger.info(['멀티프로세싱 모델데이타업데이트종료', rv])



def dedup_model_data(model):
    model.dedup_data()
    logger.info(['모델데이타중복제거완료', model])


# 메모리큐 용량초과로 실패한다. 쓰지마라
def multiprocessing_dedupModelData():
    filepath = os.path.join(CONF.PROJECT_PATH, 'src\govdatascience\dataengine\_exec_files\datamodels01.py')
    filepath = os.path.realpath(filepath)

    argv = sys.argv[1:]
    argv.append('dedup_model_data')
    models = 'AptTradeRealContract|AptRent|VillaTradeRealContract|VillaRent'
    argv.append(models)

    rv = subprocess.run([sys.executable, filepath] + argv)
    logger.info(['멀티프로세싱 모델데이타중복제거완료', rv])


def sequencial_dedupModelData():
    models = 'AptTradeRealContract|AptRent|VillaTradeRealContract|VillaRent'
    models = models.split('|')
    _len = len(models)
    for i, model in enumerate(models):
        logger.info([f'{i}/{_len}', model])
        model = MODEL_LIST[model]()
        model.dedup_data()

    logger.info(['시퀀싱 모델데이타중복제거완료'])

