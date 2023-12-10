@echo off
title API SERVER System
echo Please Check To IB Broker Program Was Open
echo Wait for start daily service.

echo Web Service run
echo Dont Close This windows

cmd /k "cd /d C:\price engain\venv\Scripts\ & activate & cd /d C:\price engain & python 1m.py"

