# -*- coding: utf-8 -*-

import os 
import subprocess
import platform 




# 스크립트 실행
# 현재 파이썬 실행 파일 경로
# python_exe = os.sys.executable
# 가상환경 파이썬 실행 파일 경로
python_exe = os.path.join(os.environ['PROJECT_PATH'], r".env\Scripts\python.exe")


# 운영 체제에 따라 커맨드창 실행 명령어 설정
if platform.system() == "Windows":
    # cmd_prefix = ["cmd", "/c", "start", "cmd", "/k"]
    cmd_prefix = ["start", "cmd", "/k"]
else:  # Linux or macOS
    cmd_prefix = ["xterm", "-e"]


# 방법-1 : 다수 파일로 실행 
def run_subprocess_by_multifile(script_files):
    _len = len(script_files)
    for i, script_file in enumerate(script_files, start=1):
        print(f"\n스크립트 실행 ({i}/{_len})-->")

        rv = subprocess.Popen(
            cmd_prefix + [python_exe, script_file], 
            shell=True
        )
        print("\n서브-프로세스 오픈 결과-->", type(rv))
        for k,v in rv.__dict__.items():
            print(f"{k}: {v}")

run_subprocess_by_multifile(
    script_files = [
        # os.path.join(os.environ['PROJECT_PATH'], r"app\tqdm\gen_code.py"),

        # os.path.join(os.environ['PROJECT_PATH'], r"app\iYouTube\api01.py"),

        # os.path.join(os.environ['PROJECT_PATH'], r"app\YouTubeHandler\unit_test.py"),

        # os.path.join(os.environ['PROJECT_PATH'], r"app\YouTubeHandler\app01.py"),
        # os.path.join(os.environ['PROJECT_PATH'], r"app\YouTubeHandler\app02.py"),
        # os.path.join(os.environ['PROJECT_PATH'], r"app\YouTubeHandler\app03.py"),
        # os.path.join(os.environ['PROJECT_PATH'], r"app\YouTubeHandler\app04.py"),
    ]
)


# 방법-2 : 한개 파일로 여러가지 함수 동시 실행 
def run_subprocess_by_onefile(script_file, func_list):
    _len = len(func_list)
    for i, (func, no) in enumerate(func_list, start=1):
        print(f"\n스크립트 실행 ({i}/{_len})-->")

        rv = subprocess.Popen(
            cmd_prefix + [python_exe, script_file, func, str(no)],
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        print("\n서브-프로세스 오픈 결과-->", type(rv))
        for k,v in rv.__dict__.items():
            print(f"{k}: {v}")

run_subprocess_by_onefile(
    script_file=os.path.join(os.environ['PROJECT_PATH'], r"app\unit_test.py"),
    func_list=[
        # ('api', 23),
        # ('test', 5),
        # ('app', 1),
        # ('app', 2),
        # ('app', 3),
        # ('app', 4),
        # ('app', 5),
        ('app', 6),
    ]
)



