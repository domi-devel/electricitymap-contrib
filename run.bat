@ECHO OFF
IF EXIST .\.env\Scripts\ (
    ECHO Starting InfluxDB parser
    .\.env\Scripts\python parse_influx.py
) ELSE (
    ECHO Venv not found.. Is everything set up correctly?
    PAUSE
)
PAUSE
