# -*- coding: utf-8 -*-
import sys 


from ipylib.idebug import *


from govdatascience.dataengine import datamngr



def initialize():
    datamngr.initialize_db()
    logger.info('프로젝트 준비완료')
    sys.exit(0)