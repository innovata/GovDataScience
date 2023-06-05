# -*- coding: utf-8 -*-
import json 
import os 

class APIKey(object):

    def __init__(self):
        file = os.path.join('C:\\pypjts', 'GovDataScience', 'credentials.json')
        with open(file, "r") as f:
            text = f.read()
        d = json.loads(text)

        class Key(object):
            def __init__(self, d): 
                for k,v in d.items(): setattr(self, k, v)

        for k,v in d.items():
            setattr(self, k, Key(v))

APIKey = APIKey()



class KoreaBankOpenAPI(object):

    def __init__(self):
        self.authKey = APIKey.BOK.AuthKey
    