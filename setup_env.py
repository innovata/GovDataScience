# -*- coding: utf-8 -*-
# 프로젝트 전체에 대한 공통 환경셋업 


print(f"\n프로젝트 전체에 대한 공통 환경셋업 ({__file__})")


import os 
import sys 
import platform 
import pprint
pp = pprint.PrettyPrinter(indent=2)
import re 



# 머신에 따른 시스템변수 설정 
COMPUTER_NAME = platform.uname().node

if COMPUTER_NAME == "RYZEN9-X570-AORUS-ELITE":
    pkgs_dir = r"D:\pypjts"
    cred_path = r"E:\내 드라이브\__CREDENTIALS__"
    ffmpeg_path = r"C:\FFmpeg\ffmpeg-7.1-full_build\bin"
elif COMPUTER_NAME == 'DP58-JLE69-KOR':
    pkgs_dir = r"F:\pypjts"
    cred_path = r"N:\My Drive\__CREDENTIALS__"
    ffmpeg_path = r"F:\__PROGRAMS_FILES_x64__\ffmpeg-2025-02-13-git-19a2d26177-full_build\bin"
elif COMPUTER_NAME == "LX3-JLE69-KOR":
    pkgs_dir = r"D:\pypjts"
    cred_path = r"H:\My Drive\__CREDENTIALS__"
    ffmpeg_path = r"C:\FFmpeg\ffmpeg-2024-12-27-git-5f38c82536-full_build\bin"
else:
    raise 


# 프로젝트 경로
os.environ['PROJECT_PATH'] = os.path.dirname(__file__)
os.environ['PROJECT_NAME'] = os.path.basename(os.environ['PROJECT_PATH'])


# 유투브 계정명
os.environ['YOUTUBE_ACCOUNT_NAME'] = "KookPpong"


# API KEY 경로설정
if COMPUTER_NAME == "RYZEN9-X570-AORUS-ELITE":
    os.environ['GOOGLE_GABRIELEHYUK_CREDENTIAL_PATH'] = r"D:\__ENGINEER_DRIVE__\__CREDENTIALS__\Google\gabrielehyuk\OAuth 2.0 Client IDs\YouTubeAgent\client_secret.json"
    os.environ['GOOGLE_AHORAGABRIELE_CREDENTIAL_PATH'] = r"D:\__ENGINEER_DRIVE__\__CREDENTIALS__\Google\ahoragabriele\OAuth 2.0 Client IDs\CreatorGabriele\client_secret.json"
    os.environ['GOOGLE_KOOKPPONG_CREDENTIAL_PATH'] = r"D:\__ENGINEER_DRIVE__\__CREDENTIALS__\Google\droga.de.nacion\OAuth 2.0 Client IDs\KookPpongAgent\client_secret.json"
elif COMPUTER_NAME == 'DP58-JLE69-KOR':
    os.environ['INSTAGRAM_CREDENTIAL_PATH'] = os.path.join(cred_path, 'INSTAGRAM', 'credentials.json')
    os.environ['GOOGLE_GABRIELEHYUK_CREDENTIAL_PATH'] = os.path.join(cred_path, 'Google', 'gabrielehyuk', 'OAuth 2.0 Client IDs', 'YouTubeAgent', 'client_secret.json')
    os.environ['GOOGLE_AHORAGABRIELE_CREDENTIAL_PATH'] = os.path.join(cred_path, 'Google', 'ahoragabriele', 'OAuth 2.0 Client IDs', 'CreatorGabriele', 'client_secret.json')
    os.environ['GOOGLE_KOOKPPONG_CREDENTIAL_PATH'] = os.path.join(cred_path, 'Google', 'droga.de.nacion', 'OAuth 2.0 Client IDs', 'KookPpongAgent', 'client_secret.json')
    os.environ['DATA_GO_KR_CREDENTIAL_PATH'] = os.path.join(cred_path, '공공데이터포털_DataGoKr', 'private_api_auth_key.json')


# iYouTube 패키지 MyPlaylist 객체 사용을 위한 셋업
os.environ['GOOGLE_CREDENTIAL_PATH'] = os.environ['GOOGLE_KOOKPPONG_CREDENTIAL_PATH']


# 크롬브라우저 북마크 경로설정 
os.environ['GABRIELE_BOOKMARKS_FILE'] = r"C:\Users\innovata\AppData\Local\Google\Chrome\User Data\Profile 1\Bookmarks"


# FFMPEG BIN 경로 저장 
os.environ['FFMPEG_BIN_LOCATION'] = ffmpeg_path



print("\n프로젝트 전역변수 셋업 상태-->")
for k, v in os.environ.items():
    if re.search(r"^COMPUTER|^PROJECT|^YOUTUBE|^GOOGLE|^FFMPEG", k):
        print(f"{k}--> '{v}'")


# My-Dev 패키지 경로 설정
for project_name in [os.environ['PROJECT_NAME'], 'iPyLibrary', 'iCrawler']:
    sys.path.append(os.path.join(pkgs_dir, project_name, 'src'))


# 프로젝트 기본경로 설정 
for folder in ['app','jupyter','src']:
    sys.path.append(os.path.join(os.environ['PROJECT_PATH'], folder))


print("\n시스템 경로-->")
pp.pprint(sorted(set(sys.path)))



def __gubun__(symbol, _len, title):
    line = symbol*_len
    print(f"\n\n{line}\n{symbol} {title}\n{line}\n\n")


def print_gubun_lv1(title, _len=100):
    __gubun__("#", _len, title)


def print_gubun_lv2(title, _len=100):
    __gubun__("+", _len, title)


def set_env_variables():
    args = sys.argv
    print("\n커맨드라인 입력변수-->")
    pp.pprint(args)

    # 커맨드라인 입력 옵션 정리
    global_vars = [
        'INSTAGRAM_CREDENTIAL_PATH',
        'GOOGLE_GABRIELEHYUK_CREDENTIAL_PATH',
        'FFMPEG_BIN_LOCATION',
    ]
    for arg in args:
        if re.search(r"^--", arg):
            arg = re.sub(r"^--", repl="", string=arg)
            k,v = arg.split('=')
            if k in global_vars:
                os.environ[k] = v
                print(f"전역변수 업데이트 완료 ({k} = {v})")