@echo off
setlocal
cd /d "%~dp0"
set PYTHONIOENCODING=utf-8
set NODE_EXE=D:\Users\Jean\.cache\codex-runtimes\codex-primary-runtime\dependencies\node\bin\node.exe
if not defined SESSION_COOKIE_NAME set SESSION_COOKIE_NAME=spo_integracao_dev_jean
if not defined SPO_ENV_LABEL set SPO_ENV_LABEL=Integracao DEV Jean
if not defined SPO_INSTANCE_PORT set SPO_INSTANCE_PORT=5001
if not defined SPO_ENV_DETAIL (
  for /f "usebackq delims=" %%B in (`git branch --show-current 2^>nul`) do set SPO_ENV_DETAIL=%%B
)
if not defined SPO_RUNNER set SPO_RUNNER=..\iniciar_flask_spo.py
".venv\Scripts\python.exe" "%SPO_RUNNER%" --app-dir "%CD%" --port %SPO_INSTANCE_PORT%
exit /b %errorlevel%
