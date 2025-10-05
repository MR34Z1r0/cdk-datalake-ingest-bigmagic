@echo off
setlocal

REM Activar el entorno virtual desde la raíz del proyecto
call D:\WORKSPACE_GIT\VALORX\cdk-datalake-ingest-upeu\.venv\Scripts\activate.bat

REM Movernos a la carpeta donde está el script y el txt
cd /c D:\WORKSPACE_GIT\VALORX\cdk-datalake-ingest-bigmagic\utils\extract_data_v2

REM Iterar sobre las tablas del archivo tablas.txt
for /f %%T in (tables.txt) do (
    echo Procesando tabla %%T...
    python main.py -t %%T -m initial
)

python execute_stage.py --process-id=10 --instance=PE --endpoint PEBDDATA2
