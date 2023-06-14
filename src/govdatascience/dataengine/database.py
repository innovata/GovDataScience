# -*- coding: utf-8 -*-
from pymongo import collection, MongoClient, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure
from pymongo.cursor import CursorType

import pandas as pd


from ipylib.idebug import *
from ipylib.datacls import BaseDataClass


from govdatascience import configuration as CONFIG




CLIENT_PARAMS = {
    'host': 'localhost',
    'port': 27017,
    'document_class': dict,
    'tz_aware': True,
    'connect': True,
    'maxPoolSize': None,
    'minPoolSize': 100,
    'connectTimeoutMS': 60000,
    'waitQueueMultiple': None,
    'retryWrites': True
}

try:
    client = MongoClient(**CLIENT_PARAMS)
    # The ismaster command is cheap and does not require auth.
    client.admin.command('ismaster')
except ConnectionFailure:
    logger.error(['ConnectionFailure:', ConnectionFailure])
    raise
else:
    db = client[CONFIG.DATABASE_NAME]


class Collection(collection.Collection):

    def __init__(self, name, create=False, **kw):
        super().__init__(db, name, create, **kw)
    @property
    def collName(self): return self.name
    def insert_data(self, data):
        try: self.insert_many(data)
        except Exception as e:
            msg = '빈데이터를 바로 인서트하는 경우는 비일비재하므로, 여기에서 경고처리한다'
            logger.warning([e, msg])
    def select(self, f, type='dict'):
        try:
            c = self.find(f, limit=1)
            d = list(c)[0]
        except Exception as e: 
            logger.warning([e, f])
            return None
        else:
            if type == 'dcls': return BaseDataClass(**d)
            elif type == 'dict': return d
    def load(self, f={}, p={}, **kwargs):
        p.update({'_id':0})
        cursor = self.find(f, p, **kwargs)
        data = list(cursor)
        print({'DataLen': len(data)})
        return data
    def view(self, f={}, p={}, **kwargs):
        cursor = self.find(f, p, **kwargs)
        data = list(cursor)
        df = pd.DataFrame(data)
        return df


def print_colunqval(m, cols):
    for c in cols:
        li = m.distinct(c)
        print(c, len(li), li if len(li) < 10 else li[:10])


def validate_collection(m): pp.pprint(db.validate_collection(m.collName))


def collection_names(pat):
    f = {'name':{'$regex':pat, '$options':'i'}}
    # f = {}
    names = db.list_collection_names(filter=f)
    print({'컬렉션Len': len(names)})
    def __view__(name):
        _names = []
        for name in names:
            _name = name.split('_')[0]
            _names.append(_name)
        _names = sorted(set(_names))
        print({'모델명': _names})

    __view__(names)
    return sorted(names)
