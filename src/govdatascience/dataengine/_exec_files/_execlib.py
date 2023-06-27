# -*- coding: utf-8 -*-
import os
import sys
from multiprocessing import Pool, freeze_support
from time import sleep


sys.path.append(os.path.join('C:\pypjts', 'GovDataScience', 'src'))


from ipylib.idebug import *





def exec_multiprocessing01(function, iterables):
    n = len(iterables)
    freeze_support()
    with Pool(n) as pool:
        # result = pool.starmap(function, iterables)
        result = set(pool.map(function, iterables))
        print(result)

        