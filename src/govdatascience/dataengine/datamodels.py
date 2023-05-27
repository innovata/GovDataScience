# -*- coding: utf-8 -*-
import os 


import pandas as pd


from ipylib.idebug import *


from govdatascience.dataengine import database



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
            # '법정동명': {'$regex': '구$|시$'},
            # '법정동명': {'$regex': '도.+시$'},
            # '지역코드': {'$regex': '[1-9]{4}0'},
            # '지역코드': {'$regex': '[1-9]{3}00'},
            '지역코드': '41480',
        }
        data = self.load(f)
        df = pd.DataFrame(data)
        df = df.drop_duplicates(subset=['지역코드'], keep='first').\
                reset_index(drop=True)
        print({'FrameLen': len(df)})
        return df 



"""아파트매매 실거래가 수집이력"""
class AptRealTradesCollectChecklist(database.Collection):

    def __init__(self): super().__init__(self.__class__.__name__)
    def __create__(self):
        f = {
            '폐지여부': '존재',
            '법정동명': {'$regex': '구$|시$'},
            '지역코드': {'$regex': '[1-9]{4}0'},
        }
        model1 = LocationCode()
        p = {c:1 for c in ['법정동명', '지역코드']}
        p.update({'_id':0})
        data = model1.load(f, p)

        dts = pd.bdate_range(start='2000/1', end='2023/6', freq='M')
        dts = sorted(dts, reverse=True)
        tradeMonths = [t.strftime("%Y%m") for t in dts]
        
        self.drop()
        for tradeMonth in tradeMonths:
            df = pd.DataFrame(data)
            df['검색연월'] = tradeMonth
            self.insert_many(df.to_dict('records'))
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
            {'$rename': {'연월': '검색연월'}}
        )




"""아파트매매 실거래 상세 자료"""
class ApartmentRealTrade(database.Collection):

    def __init__(self):
        super().__init__(self.__class__.__name__)
    def view(self):
        f = {
            '검색연월': None,
        }
        data = self.load(f)
        df = pd.DataFrame(data)
        # df = df.drop_duplicates(subset=['지역코드'], keep='first').\
        #         reset_index(drop=True)
        print({'FrameLen': len(df)})
        return df 
    

"""국토교통부_아파트 전월세 수집이력"""
class AptRentsChecklist(database.Collection):

    def __init__(self): super().__init__(self.__class__.__name__)
    def __create__(self):
        f = {
            '폐지여부': '존재',
            '법정동명': {'$regex': '구$|시$'},
            '지역코드': {'$regex': '[1-9]{4}0'},
        }
        model1 = LocationCode()
        p = {c:1 for c in ['법정동명', '지역코드']}
        p.update({'_id':0})
        data = model1.load(f, p)

        dts = pd.bdate_range(start='2000/1', end='2023/6', freq='M')
        dts = sorted(dts, reverse=True)
        tradeMonths = [t.strftime("%Y%m") for t in dts]
        
        self.drop()
        for tradeMonth in tradeMonths:
            df = pd.DataFrame(data)
            df['검색연월'] = tradeMonth
            self.insert_many(df.to_dict('records'))
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
    


"""아파트매매 실거래 상세 자료"""
class ApartmentRent(database.Collection):

    def __init__(self):
        super().__init__(self.__class__.__name__)
    def view(self):
        f = {
            # '검색연월': None,
        }
        data = self.load(f)
        df = pd.DataFrame(data)
        # df = df.drop_duplicates(subset=['지역코드'], keep='first').\
        #         reset_index(drop=True)
        print({'FrameLen': len(df)})
        return df 