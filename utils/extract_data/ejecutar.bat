@echo off
setlocal

REM Activar el entorno virtual desde la raíz del proyecto
call D:\WORKSPACE-GIT\CDK\cdk-datalake-ingest-bigmagic\.venv\Scripts\activate.bat

REM Movernos a la carpeta donde está el script y el txt
cd /d D:\WORKSPACE-GIT\CDK\cdk-datalake-ingest-bigmagic\utils\extract_data

REM Iterar sobre las tablas del archivo tablas.txt
for /f %%T in (tablas.txt) do (
    echo Procesando tabla %%T...
    python load_with_query_python.py -t %%T
)

pause
