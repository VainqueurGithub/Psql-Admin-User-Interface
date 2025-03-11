@echo off
cd /d "%~dp0"
set ENV_PATH=env

REM Check if the virtual environment exists
if not exist %ENV_PATH%\Scripts\python.exe (
    python -m venv %ENV_PATH%
    call %ENV_PATH%\Scripts\activate
    pip install -r requirements.txt
)

REM Activate the virtual environment
call %ENV_PATH%\Scripts\activate

REM Run the application without showing the console
start "" "%ENV_PATH%\Scripts\pythonw.exe" main.py
exit
