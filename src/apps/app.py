# -*- coding: utf-8 -*-
import sys
import os
from multiprocessing import Process


from ipylib.idebug import *


sys.path.append(os.path.join("C:\\pypjts", 'GovDataScience', 'src'))
from govdatascience import realestate
from govdatascience.dataengine import datamodels



def collect_RealEstate():
    realestate.collect_all_RealState()

    # datamodels.ApartmentRealTrade().clean_values_by_dtypes()

    logger.info('부동산데이터수집완료')
    sys.exit(0)




if __name__ == '__main__': 
    p = Process(target=collect_RealEstate)
    p.start()
    p.join()