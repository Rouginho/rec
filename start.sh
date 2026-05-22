#!/bin/bash
set -e

cd "$(dirname "$0")"

if [ ! -f ".env" ]; then
  echo "ERROR: .env file not found. Copy .env.example to .env and fill in your keys."
  exit 1
fi

if [ ! -d "venv" ]; then
  echo "Creating virtual environment..."
  python3 -m venv venv
fi

echo "Installing dependencies..."
venv/bin/pip install -q --upgrade pip
venv/bin/pip install -q -r requirements.txt

echo "Starting AI Reception (Sofia)..."
venv/bin/python ss.py
