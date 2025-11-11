.PHONY: build run clean install test help

# Default target
all: build

# Build the binary
build:
	@echo "Building otlp-converter..."
	@go build -ldflags="-s -w" -o otlp-converter
	@echo "✓ Built: ./otlp-converter"

# Build optimized binary
build-optimized:
	@echo "Building optimized binary..."
	@CGO_ENABLED=0 go build -ldflags="-s -w" -trimpath -o otlp-converter
	@echo "✓ Built optimized: ./otlp-converter"

# Install dependencies
install:
	@echo "Installing dependencies..."
	@go mod download
	@go mod tidy
	@echo "✓ Dependencies installed"

# Run with default settings
run:
	@go run . -input badger_export.json -output traces_otlp

# Run with custom input
run-test:
	@go run . -input ../converter_fast/badger_export.json -output traces_otlp -max 10000

# Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -f otlp-converter otlp-converter-*
	@rm -f *.arrow
	@echo "✓ Cleaned"

# Run tests
test:
	@go test -v ./...

# Format code
fmt:
	@go fmt ./...

# Run linter
lint:
	@golangci-lint run

# Cross-compile for Linux
build-linux:
	@echo "Building for Linux..."
	@GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o otlp-converter-linux
	@echo "✓ Built: ./otlp-converter-linux"

# Cross-compile for macOS
build-macos:
	@echo "Building for macOS..."
	@GOOS=darwin GOARCH=amd64 go build -ldflags="-s -w" -o otlp-converter-macos
	@echo "✓ Built: ./otlp-converter-macos"

# Build for all platforms
build-all: build-linux build-macos build

# Show help
help:
	@echo "OTLP Converter - Makefile"
	@echo ""
	@echo "Targets:"
	@echo "  make build              Build the binary"
	@echo "  make build-optimized    Build optimized binary"
	@echo "  make install            Install dependencies"
	@echo "  make run                Run with default settings"
	@echo "  make run-test           Run test with sample data"
	@echo "  make clean              Clean build artifacts"
	@echo "  make test               Run tests"
	@echo "  make fmt                Format code"
	@echo "  make lint               Run linter"
	@echo "  make build-linux        Build for Linux"
	@echo "  make build-macos        Build for macOS"
	@echo "  make build-all          Build for all platforms"
	@echo "  make help               Show this help"
	@echo ""
	@echo "Usage examples:"
	@echo "  make install && make build"
	@echo "  ./otlp-converter -input badger_export.json -output traces"
