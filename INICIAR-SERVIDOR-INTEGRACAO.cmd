@echo off
setlocal
cd /d "%~dp0"
set PYTHONIOENCODING=utf-8
set NODE_EXE=D:\Users\Jean\.cache\codex-runtimes\codex-primary-runtime\dependencies\node\bin\node.exe
set SESSION_COOKIE_NAME=spo_integracao_dev_jean
".venv\Scripts\python.exe" "..\iniciar_flask_spo.py" --app-dir "%CD%" --port 5001
exit /b %errorlevel%
