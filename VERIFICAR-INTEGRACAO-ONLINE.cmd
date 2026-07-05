@echo off
setlocal
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  echo Ambiente Python nao encontrado em .venv.
  pause
  exit /b 1
)

echo Verificando alinhamento do intermediario com o banco on-line...
echo.
echo Contexto Git atual:
git status --short --branch 2>nul
echo.
echo Esta verificacao nao troca branch e nao altera banco.
echo.
".venv\Scripts\python.exe" verificar_integracao_online.py

echo.
pause
