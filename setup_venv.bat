@Echo off
setlocal enabledelayedexpansion

@Rem Disable __pycache__ generation
setx PYTHONDONTWRITEBYTECODE 1

@Rem Create virtual environment
python -m venv venv

@Rem Install deps
.\venv\Scripts\python.exe -m pip install -r requirements.txt