"""
[bat 파일]

"C:\pypjts\GovDataScience\govDataScienceEnv_py39_64bit\Scripts\python.exe" "C:\pypjts\GovDataScience\src\apps\script.py"
pause
"""




import subprocess


"""64비트 앱실행"""
python_bin = 'C:\\pypjts\\GovDataScience\\govDataScienceEnv_py39_64bit\\Scripts\\python.exe'
script_file = 'C:\\pypjts\\GovDataScience\\src\\apps\\app.py'
subprocess.Popen([python_bin, script_file])
