# -*- coding: utf-8 -*-
from multiprocessing import Process, Pool, freeze_support


from ipylib.idebug import *


from govdatascience import init_pjt, realestate
from govdatascience.dataengine import datamodels




class MainSystem(object):
    def __init__(self): 
        self.seq = 0 
    def _set_process(self, process):
        self.seq += 1
        setattr(self, str(self.seq), process)
    def initialize(self):
        p = Process(target=init_pjt.initialize)
        self._set_process(p)
        p.start()
        p.join()
    def run(self):
        targets = [
            realestate.collect_all_RealState,
            # datamodels.multiprocessing_updateModelData,
            # datamodels.sequencial_dedupModelData,
        ]
        for target in targets:
            p = Process(target=target)
            self._set_process(p)
            p.start()
            p.join()
            
    




