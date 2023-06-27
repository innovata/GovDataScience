# -*- coding: utf-8 -*-


from _execlib import *


from govdatascience.dataengine import datamodels




def __task01__(funcName, modelName):
    func = getattr(datamodels, funcName)
    model = getattr(datamodels, modelName)()
    # for i in range(5): sleep(1)
    # logger.info([func, model])
    func(model)

def update_model_data(modelName):
    # logger.info(modelName)
    __task01__('update_model_data', modelName)


def dedup_model_data(modelName):
    # logger.info(modelName)
    __task01__('dedup_model_data', modelName)





FUNCTIONS = vars()




if __name__ == '__main__':
    pp.pprint({'__file__': __file__, 'sys.argv': sys.argv})
    
    funcName = sys.argv[1]
    modelNames = sys.argv[2]
    modelNames = modelNames.split('|')
    pp.pprint(modelNames)

    n = len(modelNames)
    freeze_support()
    with Pool(n) as pool:
        function = FUNCTIONS[funcName]
        result = pool.map(function, modelNames)
        print(result)

