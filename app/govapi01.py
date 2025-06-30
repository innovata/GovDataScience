# -*- coding: utf-8 -*-



import os, sys 


# 환경셋업
sys.path.append(os.environ['PROJECT_PATH'])
from setup_env import * 


from ipylib.idebug import *


"""커멘드라인 입력변수처리"""
os.environ['RUN_FILE_PATH'] = __file__







if __name__ == '__main__': 
    ms = main.MainSystem()
    # ms.initialize()
    ms.run()
    sys.exit(0)


    