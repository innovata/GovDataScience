# -*- coding: utf-8 -*-

SCRIPT_TITLE = "ytviewer 패키지에 대한 단위함수 테스트"
print(f"\n\n{'#'*100}\n\n{[__file__, SCRIPT_TITLE]}\n\n{'#'*100}\n\n")


import os, sys 
from datetime import datetime, timedelta


# 환경셋업
sys.path.append(os.environ['KOOK_PPONG_PROJECT_PATH'])
from setup_env import *


from pymongo import MongoClient 
client = MongoClient()


from ipylib import ifile 



from govapi import datagokr 




os.environ['RUN_FILE_PATH'] = __file__




class GovAPI:

    def api01(self):
        datagokr.get_data(
            api_name="아파트매매 실거래",
            locationCode:str, 
            tradeMonth:str=None, 
            n_rows:int=1000
        )


class UnitTester:

    # 플레이리스트 생성
    def test01(self):
        # ytviewer.build_all_playlists()
        ytviewer.generate_deep_news_playlist()

    # 채널
    def test02(self):
        # ytviewer.get_n_save_my_subscriptions()
        
        # ytviewer.get_n_save_channel_its_videos(
        #     "https://www.youtube.com/@understanding.",
        #     category="Gen_경제전망"
        # )
        
        ytviewer.get_n_save_collectable_channels()

        # ytviewer.add_channel("https://www.youtube.com/@kpunch")

    # Data Query
    def test03(self):
        # ytviewer.save_policy_data()
        # dquery.update_query_regex_channel_from_policy()
        # return 

        # # 채널
        urls = dquery.credible_channel_urls()
        print("\n신뢰할만한 채널 URL 개수-->", len(urls))

        _regex = dquery.query_regex_for_channel_name('Gen_비트코인')
        print("\nQuery Regex-->", _regex)

        urls = dquery.query_channel_urls('Gen_비트코인')
        print("\n채널 URL 개수-->", len(urls))


        # 비디오 
        _regex = dquery.query_regex_for_video_title("Gen_비트코인")
        print("\nQuery Regex-->", _regex)

        ids = dquery.query_videos_ids('Gen_비트코인', days=7)
        print("\n비디오ID 개수-->", len(ids))

    # 검색
    def test04(self):
        ytviewer.search_n_save_results()

    # Policy Data 
    def test05(self):
        # data = policy_data.copy_COLLECTABLE_CHANNELS()
        # print("\n선택적 채널들--> (한글이 잘 표현되는지 확인할 것)")
        # pp.pprint(data)

        ytviewer.save_policy_data(drop=True)
        ytviewer.auto_update_collectable_channels()



class Applications:

    def _print_test_log(self):
        tester = UnitTester()
        print(tester)


    # 유투브 검색 결과 DB 저장
    def app01(self):
        # self._print_test_log()
        ytviewer.search_n_save_results()

    # 채널정보 & 채널영상 수집/DB저장
    def app02(self):
        # self._print_test_log()
        ytviewer.get_n_save_collectable_channels()
        # ytviewer.get_n_save_playlist_channels_its_videos(playlist_name='Gen_부동산')

    # 채널 (수동적 선택)
    def app03(self):
        ytviewer.get_n_save_my_subscriptions()

    # Gen_플레이리스트 업데이트
    def app04(self):
        ytviewer.build_all_playlists()

    def app05(self):
        ytviewer.get_n_save_other_playlists_its_videos()

    # MY플레이리스트
    def app06(self):
        ytviewer.get_n_save_my_playlists_its_items()




class Executor:

    def api(self, no:str):
        api = GovAPI()
        method_name = f"api{no.zfill(2)}"
        getattr(api, method_name)()

    def test(self, no:str):
        tester = UnitTester()
        method_name = f"test{no.zfill(2)}"
        getattr(tester, method_name)()

    def app(self, no:str):
        app = Applications()
        method_name = f"app{no.zfill(2)}"
        getattr(app, method_name)()




if __name__ == "__main__":
    print_gubun_lv1("메인시작...")
    print_gubun_lv2("서브시작...")
    set_env_variables()


    func_name, func_no = sys.argv[1:]
    executor = Executor()
    getattr(executor, func_name)(func_no)


    print("\n프로세스 종료")
    sys.exit(1)



