@echo off

set /p json_file=Enter JSON file name: 
set /p columns=Enter columns (space-separated): 

set PYTHON_EXE="C:\Users\suren\AppData\Local\Programs\Python\Python312\python.exe"
set SCRIPT_PATH="C:\Users\suren\PycharmProjects\ETL Automation_CITI\TestSuite\Domaincheck1.py"
set CONFIG_PATH="C:\Users\suren\PycharmProjects\ETL Automation_CITI\ConfigFiles_Scenarios"

%PYTHON_EXE% %SCRIPT_PATH% -f %json_file% -c %columns%

pause