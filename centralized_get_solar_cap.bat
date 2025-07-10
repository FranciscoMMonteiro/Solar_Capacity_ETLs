@echo off

REM Save the current directory
set CURRENT_DIR=%~dp0

REM Activate the Conda environment
call activate pyQuant_3_11

REM Run the Python script
python "%CURRENT_DIR%\centralized_get_solar_cap.py"

REM Pause the script to keep the window open
pause