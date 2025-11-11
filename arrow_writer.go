package main

import (
	"fmt"
	"os"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/apache/arrow/go/v14/arrow/memory"
)

type ArrowRow struct {
	OTLPSpan    string
	TraceID     string
	SpanID      string
	ServiceName string
	Name        string
}

// WriteArrowFile writes OTLP spans to Arrow IPC file format
func WriteArrowFile(filename string, rows []ArrowRow) error {
	// Define Arrow schema matching Python format
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "otlp_span", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "trace_id", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "span_id", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "service_name", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		},
		nil,
	)

	// Create memory allocator
	mem := memory.NewGoAllocator()

	// Build record
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	// Populate columns
	otlpSpanBuilder := builder.Field(0).(*array.StringBuilder)
	traceIDBuilder := builder.Field(1).(*array.StringBuilder)
	spanIDBuilder := builder.Field(2).(*array.StringBuilder)
	serviceNameBuilder := builder.Field(3).(*array.StringBuilder)
	nameBuilder := builder.Field(4).(*array.StringBuilder)

	for _, row := range rows {
		otlpSpanBuilder.Append(row.OTLPSpan)
		traceIDBuilder.Append(row.TraceID)
		spanIDBuilder.Append(row.SpanID)
		serviceNameBuilder.Append(row.ServiceName)
		nameBuilder.Append(row.Name)
	}

	record := builder.NewRecord()
	defer record.Release()

	// Write to file using Arrow IPC format (Feather v2)
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Create IPC writer with compression
	writer, err := ipc.NewFileWriter(
		file,
		ipc.WithSchema(schema),
		ipc.WithAllocator(mem),
		ipc.WithLZ4(),
	)
	if err != nil {
		return fmt.Errorf("failed to create Arrow writer: %w", err)
	}
	defer writer.Close()

	// Write record
	if err := writer.Write(record); err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	return nil
}
