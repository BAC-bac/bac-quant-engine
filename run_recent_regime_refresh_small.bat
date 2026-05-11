@echo off
cd /d C:\Users\benco\PycharmProjects\BAC_Quant_Engine

C:\Users\benco\PycharmProjects\BAC_Quant_Engine\.venv\Scripts\python.exe scripts\regimes\10_run_regime_recent_refresh.py --mode small --lookback-bars 1000

exit /b %ERRORLEVEL%