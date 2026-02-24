@echo off
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
  echo No se encontro el entorno virtual en .venv\Scripts\python.exe
  echo Cree el entorno e instale dependencias antes de ejecutar.
  pause
  exit /b 1
)

echo Iniciando aplicacion...
".venv\Scripts\python.exe" app.py
pause
