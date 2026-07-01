@echo off
setlocal
cd /d "%~dp0"

echo Atualizando codigo do ambiente intermediario dev/jean.
echo.

powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0atualizar_codigo_dev_jean.ps1"
if errorlevel 1 goto erro

echo.
echo Codigo intermediario atualizado.
pause
exit /b 0

:erro
echo.
echo Nao foi possivel concluir a atualizacao.
echo Veja a mensagem acima. Nenhuma alteracao local foi descartada por este script.
pause
exit /b 1
