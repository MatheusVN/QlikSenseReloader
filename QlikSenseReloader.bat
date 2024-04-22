@echo off

set VENV_NAME=C:\Workspace\QlikSenseReloader\env

if not exist "%VENV_NAME%" (
    python -m virtualenv %VENV_NAME%
)

REM
call %VENV_NAME%\Scripts\activate

python C:\Workspace\QlikSenseReloader\QlikSenseReloader.py

deactivate