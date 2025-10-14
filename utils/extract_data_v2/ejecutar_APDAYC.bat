@echo off
setlocal

REM ===========================================================
REM  Script: ingest_normal.bat
REM  Descripción: Ejecuta la carga normal de tablas para BigMagic
REM  Autor: Miguel Espinoza
REM  Fecha: %date% %time%
REM ===========================================================

REM ---- Detectar ruta base automáticamente ----
set PROJECT_DIR=D:\WORKSPACE-GIT\VALORX\cdk-datalake-ingest-bigmagic
set SCRIPT_DIR=%PROJECT_DIR%\utils\extract_data_v2
set VENV_PYTHON=%PROJECT_DIR%\.venv\Scripts\python.exe

REM ---- Cambiar al directorio donde están los scripts ----
cd /d "%SCRIPT_DIR%"

REM ---- Verificar entorno virtual ----
if not exist "%VENV_PYTHON%" (
    echo ERROR: No se encontró el entorno virtual en "%VENV_PYTHON%"
    exit /b 1
)

REM ---- Iterar sobre las tablas del archivo tables.txt ----
set PYTHONIOENCODING=utf-8
for /f %%T in (tables.txt) do (
    echo Procesando tabla %%T...
    "%VENV_PYTHON%" main.py -t %%T -m normal
)

REM ---- Ejecutar etapa final ----
echo Ejecutando etapa final...
"%VENV_PYTHON%" execute_stage.py --process-id=10 --instance=PE

echo Proceso finalizado correctamente.
endlocal
exit /b 0
