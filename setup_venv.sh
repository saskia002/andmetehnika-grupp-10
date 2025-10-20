#!/bin/bash
set -e

# Disable __pycache__ generation
export PYTHONDONTWRITEBYTECODE=1

# Create virtual environment
python3 -m venv venv

# Install dependencies
./venv/bin/python -m pip install -r requirements.txt
