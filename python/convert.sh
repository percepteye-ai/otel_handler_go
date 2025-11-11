#!/bin/bash
# Wrapper script to convert Arrow files to OTLP format

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Activate virtual environment
if [ ! -d "venv" ]; then
    echo "ERROR: Virtual environment not found. Run: python3 -m venv venv"
    exit 1
fi

source venv/bin/activate

# Run the conversion script
python convert_arrow_to_otlp.py "$@"

