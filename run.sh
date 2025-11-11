#!/bin/bash

# One-command runner for OTLP Converter
set -e

echo "====================================================================="
echo "OTLP Converter - Auto Setup & Run"
echo "====================================================================="
echo ""

# Check Go
if ! command -v go &> /dev/null; then
    echo "ERROR: Go not installed!"
    echo "Install: sudo apt install golang-go"
    exit 1
fi

echo "✓ Go: $(go version)"
echo ""

# Install dependencies if needed
if [ ! -d "vendor" ] && [ ! -f "go.sum" ]; then
    echo "Installing dependencies..."
    go mod download
    go mod tidy
    echo "✓ Dependencies installed"
    echo ""
fi

# Build if binary doesn't exist or source is newer
if [ ! -f "otlp-converter" ] || [ "main.go" -nt "otlp-converter" ]; then
    echo "Building binary..."
    go build -ldflags="-s -w" -o otlp-converter
    echo "✓ Built: otlp-converter"
    echo ""
fi

# Find input file
INPUT_FILE=""
if [ -f "../converter_fast/badger_export.json" ]; then
    INPUT_FILE="../converter_fast/badger_export.json"
elif [ -f "badger_export.json" ]; then
    INPUT_FILE="badger_export.json"
else
    echo "ERROR: Cannot find badger_export.json"
    echo "Expected locations:"
    echo "  - ../converter_fast/badger_export.json"
    echo "  - ./badger_export.json"
    exit 1
fi

echo "Found input: $INPUT_FILE"
echo ""

# Run converter
echo "====================================================================="
echo "Starting conversion..."
echo "====================================================================="
echo ""

./otlp-converter -input "$INPUT_FILE" -output traces_otlp

echo ""
echo "====================================================================="
echo "✓ Done! Output files: traces_otlp.batch_*.arrow"
echo "====================================================================="
