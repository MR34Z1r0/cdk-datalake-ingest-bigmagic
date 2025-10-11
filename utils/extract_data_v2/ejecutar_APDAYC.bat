@echo off
setlocal

REM ===========================================================
REM  Script: ingest_normal.bat
REM  Descripción: Ejecuta la carga inicial de tablas para BigMagic
REM  Autor: Miguel Espinoza
REM  Fecha: %date% %time%
REM ===========================================================

REM ---- Detectar ruta base automáticamente ----
REM %~dp0 obtiene la ruta del directorio donde está este .bat
REM .. sube un nivel al directorio raíz del proyecto
set PROJECT_DIR=D:\WORKSPACE-GIT\VALORX\cdk-datalake-ingest-bigmagic
set SCRIPT_DIR=%PROJECT_DIR%\utils\extract_data_v2
set LOG_DIR=%SCRIPT_DIR%\logs
set LOG_FILE=%LOG_DIR%\log_ingest_normal.txt
set VENV_PYTHON=%PROJECT_DIR%\.venv\Scripts\python.exe

REM ---- Crear carpeta de logs si no existe ----
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"


REM ---- Crear carpeta de logs si no existe ----
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

REM ---- Cambiar al directorio donde están los scripts ----
cd /d "%SCRIPT_DIR%"

REM ---- Iniciar log ----
echo =========================================================== >> "%LOG_FILE%"
echo [%date% %time%] Iniciando proceso de ingestión inicial >> "%LOG_FILE%"
echo =========================================================== >> "%LOG_FILE%"

REM ---- Verificar entorno virtual ----
if not exist "%VENV_PYTHON%" (
    echo [%date% %time%] ERROR: No se encontró el entorno virtual en "%VENV_PYTHON%" >> "%LOG_FILE%"
    exit /b 1
)

REM ---- Iterar sobre las tablas del archivo tables.txt ----
set PYTHONIOENCODING=utf-8
for /f %%T in (tables.txt) do (
    echo [%date% %time%] Procesando tabla %%T... >> "%LOG_FILE%"
    "%VENV_PYTHON%" main.py -t %%T -m normal >> "%LOG_FILE%" 2>&1
)

REM ---- Ejecutar etapa final ----
echo [%date% %time%] Ejecutando etapa final... >> "%LOG_FILE%"
"%VENV_PYTHON%" execute_stage.py --process-id=10 --instance=PE >> "%LOG_FILE%" 2>&1

REM ---- Finalizar ----
echo [%date% %time%] Proceso finalizado correctamente. >> "%LOG_FILE%"
echo =========================================================== >> "%LOG_FILE%"
echo. >> "%LOG_FILE%"
endlocal
exit /b 0
