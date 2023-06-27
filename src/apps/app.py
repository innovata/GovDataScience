# -*- coding: utf-8 -*-
import sys
import os


from ipylib.idebug import *


"""커멘드라인 입력변수처리"""
os.environ['RUN_FILE_PATH'] = __file__


"""프로젝트 패키지/모듈 셋업"""
sys.path.append(os.path.join("C:\\pypjts", 'GovDataScience', 'src'))
from govdatascience import main




if __name__ == '__main__': 
    ms = main.MainSystem()
    # ms.initialize()
    ms.run()
    sys.exit(0)


    