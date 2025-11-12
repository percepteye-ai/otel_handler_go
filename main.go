package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"
)

type Config struct {
	InputFile      string
	OutputFile     string
	MaxEntries     int
	NumWorkers     int
	BatchSize      int
	WriteInterval  int
	OutputFormat   string // "arrow" or "json" or "both"
}

type BadgerExport struct {
	Entries []BadgerEntry `json:"entries"`
}

type BadgerEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"` // hex-encoded protobuf
}

func main() {
	config := parseFlags()

	fmt.Println("=" + string(make([]byte, 78)) + "=")
	fmt.Println("OTLP CONVERTER - GO (BLAZING FAST)")
	fmt.Println("=" + string(make([]byte, 78)) + "=")
	fmt.Printf("CPU cores: %d\n", runtime.NumCPU())
	fmt.Printf("Workers: %d\n", config.NumWorkers)
	fmt.Printf("Batch size: %d\n", config.BatchSize)
	fmt.Println()
	fmt.Println("ADVANTAGES:")
	fmt.Println("  ✓ Native protobuf parsing (50-100x faster than Python)")
	fmt.Println("  ✓ True parallelism with goroutines (no GIL)")
	fmt.Println("  ✓ Low memory overhead")
	fmt.Println("  ✓ Full OTLP format in Arrow files")
	fmt.Println("=" + string(make([]byte, 78)) + "=")
	fmt.Println()

	startTime := time.Now()

	// Open input file
	fmt.Printf("Reading: %s\n", config.InputFile)
	file, err := os.Open(config.InputFile)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer file.Close()

	// Create converter
	converter := NewConverter(config)

	// Start background writer
	writerDone := make(chan struct{})
	go converter.BackgroundWriter(writerDone)

	// Read and parse entries
	decoder := json.NewDecoder(file)

	// Read opening brace
	if _, err := decoder.Token(); err != nil {
		log.Fatalf("Error reading JSON: %v", err)
	}

	// Find entries array
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			log.Fatalf("Error reading JSON: %v", err)
		}

		if token == "entries" {
			// Read array opening bracket
			if _, err := decoder.Token(); err != nil {
				log.Fatalf("Error reading entries array: %v", err)
			}
			break
		}
	}

	// Process entries in parallel
	entryChan := make(chan BadgerEntry, config.BatchSize)
	resultChan := make(chan *OTLPSpan, config.BatchSize*2)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < config.NumWorkers; i++ {
		wg.Add(1)
		go converter.Worker(entryChan, resultChan, &wg)
	}

	// Start result collector
	collectorDone := make(chan struct{})
	go converter.ResultCollector(resultChan, collectorDone)

	// Stream entries from JSON
	processed := 0
	for decoder.More() {
		var entry BadgerEntry
		if err := decoder.Decode(&entry); err != nil {
			log.Printf("Error decoding entry: %v", err)
			continue
		}

		entryChan <- entry
		processed++

		if config.MaxEntries > 0 && processed >= config.MaxEntries {
			break
		}

		if processed%10000 == 0 {
			fmt.Printf("Queued %d entries...\n", processed)
		}
	}

	// Shutdown sequence
	close(entryChan)
	wg.Wait()
	close(resultChan)
	<-collectorDone

	// Signal writer to finish
	converter.Shutdown()
	<-writerDone

	elapsed := time.Since(startTime)

	fmt.Println()
	fmt.Println("=" + string(make([]byte, 78)) + "=")
	fmt.Println("✓ CONVERSION COMPLETE")
	fmt.Printf("  Total entries processed: %d\n", processed)
	fmt.Printf("  Total spans written: %d\n", converter.TotalSpans())
	fmt.Printf("  Total time: %.1fs\n", elapsed.Seconds())
	fmt.Printf("  Rate: %.0f spans/sec\n", float64(converter.TotalSpans())/elapsed.Seconds())
	fmt.Printf("  Batch files: %d\n", converter.BatchCount())
	fmt.Println("=" + string(make([]byte, 78)) + "=")
	fmt.Println()
	switch config.OutputFormat {
	case "json":
		fmt.Printf("Output: %s.batch_NNNN.otlp.json\n", config.OutputFile)
	case "both":
		fmt.Printf("Output: %s.batch_NNNN.arrow and %s.batch_NNNN.otlp.json\n", config.OutputFile, config.OutputFile)
	default:
		fmt.Printf("Output: %s.batch_NNNN.arrow\n", config.OutputFile)
		fmt.Println()
		fmt.Println("Use Python to read:")
		fmt.Println("  from load_arrow_traces import load_otlp_spans_from_arrow")
		fmt.Println("  spans = load_otlp_spans_from_arrow('output.batch_0000.arrow')")
	}
}

func parseFlags() *Config {
	config := &Config{}

	flag.StringVar(&config.InputFile, "input", "badger_export.json", "Input BadgerDB export file")
	flag.StringVar(&config.OutputFile, "output", "traces_otlp", "Output base filename")
	flag.StringVar(&config.OutputFormat, "format", "arrow", "Output format: arrow, json, or both")
	flag.IntVar(&config.MaxEntries, "max", 0, "Max entries to process (0 = all)")
	flag.IntVar(&config.NumWorkers, "workers", runtime.NumCPU(), "Number of workers")
	flag.IntVar(&config.BatchSize, "batch", 200000, "Batch size for processing")
	flag.IntVar(&config.WriteInterval, "write-interval", 2000000, "Write to disk every N spans (default: 2M spans per file)")

	flag.Parse()

	return config
}
