import sys
import os
sys.path.append(os.path.join("C:\\pypjts", 'GovDataScience', 'src'))


from govdatascience import realestate
from govdatascience.dataengine import datamodels




# locationPat = '송파구$'
# realestate.collect_RealState(data_type='아파트매매 실거래', locationPat=locationPat)
# realestate.collect_RealState(data_type='아파트 전월세 자료', locationPat=locationPat)
# realestate.collect_RealState(data_type='연립다세대 매매 실거래자료', locationPat=locationPat)
# realestate.collect_RealState(data_type='연립다세대 전월세', locationPat=locationPat)
realestate.collect_all_RealState()


# datamodels.ApartmentRealTrade().clean_values_by_dtypes()
# datamodels.multiprocessing_updateModelData()
# datamodels.sequencial_dedupModelData()
