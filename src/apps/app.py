# -*- coding: utf-8 -*-
import sys
import os
from multiprocessing import Process


from ipylib.idebug import *


sys.path.append(os.path.join("C:\\pypjts", 'GovDataScience', 'src'))
from govdatascience import realestate
from govdatascience.dataengine import datamodels



def collect_realEstate():
    realestate.collect_RealState(data_type='아파트매매 실거래')
    realestate.collect_RealState(data_type='아파트 전월세 자료')
    realestate.collect_RealState(data_type='연립다세대 매매 실거래자료')
    realestate.collect_RealState(data_type='연립다세대 전월세')

    # datamodels.ApartmentRealTrade().clean_values_by_dtypes()

    logger.info('부동산데이터수집완료')
    sys.exit(0)




if __name__ == '__main__': 
    p = Process(target=collect_realEstate)
    p.start()
    p.join()