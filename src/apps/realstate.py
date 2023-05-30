import sys
import os
sys.path.append(os.path.join("C:\\pypjts", 'GovDataScience', 'src'))


from govdatascience import realstate
from govdatascience.dataengine import datamodels


realstate.collect_RealState(data_type='아파트매매 실거래')
realstate.collect_RealState(data_type='아파트 전월세 자료')
realstate.collect_RealState(data_type='연립다세대 매매 실거래자료')
realstate.collect_RealState(data_type='연립다세대 전월세')

# datamodels.ApartmentRealTrade().clean_values_by_dtypes()