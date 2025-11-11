#!/bin/bash

# OTLP Converter - Quick Start Script

set -e

echo "====================================================================="
echo "OTLP Converter - Quick Start"
echo "====================================================================="
echo ""

# Check if Go is installed
if ! command -v go &> /dev/null; then
    echo "ERROR: Go is not installed!"
    echo ""
    echo "Install Go first:"
    echo "  Ubuntu/Debian: sudo apt install golang-go"
    echo "  macOS: brew install go"
    echo "  Or download from: https://go.dev/dl/"
    echo ""
    exit 1
fi

echo "✓ Go is installed: $(go version)"
echo ""

# Install dependencies
echo "Installing dependencies..."
go mod download
go mod tidy
echo "✓ Dependencies installed"
echo ""

# Build binary
echo "Building optimized binary..."
CGO_ENABLED=0 go build -ldflags="-s -w" -trimpath -o otlp-converter
echo "✓ Binary built: ./otlp-converter"
echo ""

# Show binary info
if [ -f otlp-converter ]; then
    size=$(du -h otlp-converter | cut -f1)
    echo "Binary size: $size"
    echo ""
fi

# Show usage
echo "====================================================================="
echo "Ready to use!"
echo "====================================================================="
echo ""
echo "Basic usage:"
echo "  ./otlp-converter -input badger_export.json -output traces_otlp"
echo ""
echo "With options:"
echo "  ./otlp-converter \\"
echo "    -input badger_export.json \\"
echo "    -output traces_otlp \\"
echo "    -workers 16 \\"
echo "    -batch 200000"
echo ""
echo "Test run (first 10k entries):"
echo "  ./otlp-converter -input badger_export.json -max 10000"
echo ""
echo "See README.md for more information"
echo ""
